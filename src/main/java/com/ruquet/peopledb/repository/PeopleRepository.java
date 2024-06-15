package com.ruquet.peopledb.repository;

import com.ruquet.peopledb.anotation.SQL;
import com.ruquet.peopledb.model.Address;
import com.ruquet.peopledb.model.CrudOperation;
import com.ruquet.peopledb.model.Person;
import com.ruquet.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class PeopleRepository extends CrudRepository<Person> {
    private AdressesRepository adressesRepository = null;
    public static final String SAVE_PERSON_SQL = "INSERT INTO PEOPLE (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BIZ_ADDRESS, SPOUSE, PARENT_ID) VALUES(?,?,?,?,?,?,?,?,?)";
    public static final String FIND_BY_ID = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY, HOME_ADDRESS, BIZ_ADDRESS FROM PEOPLE WHERE ID = ?";
    public static final String FIND_ALL_SQL = """
               SELECT 
               PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME, PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.HOME_ADDRESS AS PARENT_HOME_ADDRESS, PARENT.BIZ_ADDRESS AS PARENT_BIZ_ADDRESS
               FROM PEOPLE AS PARENT
               FETCH FIRST 100 ROWS ONLY
            """;
    public static final String SELECT_COUNT_SQL = "SELECT COUNT(*) FROM PEOPLE";
    public static final String DELETE_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_MANY_SQL = "DELETE FROM PEOPLE WHERE ID IN(:ids)";
    public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=?, SALARY=? WHERE ID=?";

    private Map<String, Integer> aliasColIdxMap = new HashMap<>();


    public PeopleRepository(Connection connection) throws SQLException {
        super(connection);
        adressesRepository = new AdressesRepository(connection);
    }

    @Override
    @SQL(value = SAVE_PERSON_SQL, operationType = CrudOperation.SAVE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
//        Address savedAddress = null;
//        Person spouse = null;
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimeStamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());
        associateAddressWithPerson(ps, entity.getHomeAddress(), 6);
        associateAddressWithPerson(ps, entity.getBusinessAddress(), 7);
        associateSpouseWithPerson(ps, entity.getSpouse(), 8);
        associatePersonWithChildren(entity, ps);
    }

    private static void associatePersonWithChildren(Person entity, PreparedStatement ps) throws SQLException {
        Optional<Person> parent = entity.getParent();
        if (parent.isPresent()) {
            ps.setLong(9, parent.get().getId());
        } else ps.setObject(9, null);
    }

    private void associateAddressWithPerson(PreparedStatement ps, Optional<Address> address, int parameterIndex) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = adressesRepository.save(address.get());
            ps.setLong(parameterIndex, savedAddress.id());
        } else {
            ps.setObject(parameterIndex, null);
        }
    }

    @Override
    protected void postSave(Person entity, long id) {
        entity.getChildren().stream().forEach(this::save);

    }

    private void associateSpouseWithPerson(PreparedStatement ps, Optional<Person> spouse, int parameterIndex) throws SQLException {
        Person savedSpouse;
        if (spouse.isPresent()) {
            savedSpouse = save(spouse.get());
            ps.setLong(parameterIndex, savedSpouse.getId());
        } else {
            ps.setObject(parameterIndex, null);
        }
    }


    @Override
    @SQL(value = UPDATE_SQL, operationType = CrudOperation.UPDATE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimeStamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }

    @Override
    @SQL(value = """
              SELECT
              PARENT.ID AS PARENT_ID, PARENT.FIRST_NAME AS PARENT_FIRST_NAME, PARENT.LAST_NAME AS PARENT_LAST_NAME, PARENT.DOB AS PARENT_DOB, PARENT.SALARY AS PARENT_SALARY, PARENT.EMAIL AS PARENT_EMAIL, PARENT.HOME_ADDRESS AS PARENT_HOME_ADDRESS, PARENT.BIZ_ADDRESS AS PARENT_BIZ_ADDRESS,
              HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_ADDRESS2, HOME.CITY AS HOME_CITY, HOME.STATE AS HOME_STATE, HOME.POSTCODE AS HOME_POSTCODE, HOME.COUNTY AS HOME_COUNTY, HOME.REGION AS HOME_REGION, HOME.COUNTRY AS HOME_COUNTRY,
              BIZ.ID AS BIZ_ID, BIZ.STREET_ADDRESS AS BIZ_STREET_ADDRESS, BIZ.ADDRESS2 AS BIZ_ADDRESS2, BIZ.CITY AS BIZ_CITY, BIZ.STATE AS BIZ_STATE, BIZ.POSTCODE AS BIZ_POSTCODE, BIZ.COUNTY AS BIZ_COUNTY, BIZ.REGION AS BIZ_REGION, BIZ.COUNTRY AS BIZ_COUNTRY,
              SPOUSE.ID AS SPOUSE_ID, SPOUSE.FIRST_NAME AS SPOUSE_FIRST_NAME, SPOUSE.LAST_NAME AS SPOUSE_LAST_NAME, SPOUSE.DOB AS SPOUSE_DOB, SPOUSE.SALARY AS SPOUSE_SALARY, SPOUSE.HOME_ADDRESS AS SPOUSE_HOME_ADDRESS, SPOUSE.BIZ_ADDRESS AS SPOUSE_BIZ_ADDRESS,
              CHILDREN.ID AS CHILDREN_ID, CHILDREN.FIRST_NAME AS CHILDREN_FIRST_NAME, CHILDREN.LAST_NAME AS CHILDREN_LAST_NAME, CHILDREN.DOB AS CHILDREN_DOB, CHILDREN.SALARY AS CHILDREN_SALARY, CHILDREN.EMAIL AS CHILDREN_EMAIL, CHILDREN.HOME_ADDRESS AS CHILDREN_HOME_ADDRESS, CHILDREN.BIZ_ADDRESS AS CHILDREN_BIZ_ADDRESS
              FROM PEOPLE AS PARENT
              LEFT OUTER JOIN ADDRESSES AS HOME ON PARENT.HOME_ADDRESS = HOME.ID
              LEFT OUTER JOIN ADDRESSES AS BIZ ON PARENT.BIZ_ADDRESS = BIZ.ID
              LEFT OUTER JOIN PEOPLE SPOUSE ON PARENT.SPOUSE = SPOUSE.ID            
              LEFT OUTER JOIN PEOPLE CHILDREN ON PARENT.ID = CHILDREN.PARENT_ID
              WHERE PARENT.ID=?
            """, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_SQL, operationType = CrudOperation.DELETE_ONE)
    @SQL(value = DELETE_MANY_SQL, operationType = CrudOperation.DELETE_MANY)
    protected Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        Person finalParent = null;
        do {
            Person currentParent = extractPerson(rs, "PARENT_").get();
            if (finalParent == null) {
                finalParent = currentParent;
            }
            if (!finalParent.equals(currentParent)) {
                rs.previous();
                break;
            }
            Optional<Person> spouse = extractPerson(rs, "SPOUSE_");
            Optional<Person> child = extractPerson(rs, "CHILDREN_");

            Address homeAddress = extractAddres(rs, "HOME_");
            Address bizAddress = extractAddres(rs, "BIZ_");
            finalParent.setHomeAddress(homeAddress);
            finalParent.setBusinessAddress(bizAddress);

            spouse.ifPresent(finalParent::setSpouse);
            child.ifPresent(finalParent::addChild);
        } while (rs.next());
        return finalParent;
    }

    private Optional<Person> extractPerson(ResultSet rs, String alias) throws SQLException {
        Long peopleID = getValueByAlias(alias + "ID", rs, Long.class);
        if (peopleID == null) {
            return Optional.empty();
        }
        String firstName = getValueByAlias(alias + "FIRST_NAME", rs, String.class);
        String lastName = getValueByAlias(alias + "LAST_NAME", rs, String.class);
        ZonedDateTime dob = ZonedDateTime.of(getValueByAlias(alias + "DOB", rs, Timestamp.class).toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = getValueByAlias(alias + "SALARY", rs, BigDecimal.class);
        Person person = new Person(peopleID, firstName, lastName, dob, salary);
        return Optional.of(person);
    }

    private Address extractAddres(ResultSet rs, String alias) throws SQLException {
        Long addrId = getValueByAlias(alias + "ID", rs, Long.class);
        if (addrId == null) {return null;}
//        long addrId = rs.getLong("A_ID");
        String streetAddress = getValueByAlias(alias + "STREET_ADDRESS", rs, String.class);
        String address2 = getValueByAlias(alias + "ADDRESS2", rs, String.class);
        String city = getValueByAlias(alias + "CITY", rs, String.class);
        String state = getValueByAlias(alias + "STATE", rs, String.class);
        String postCode = getValueByAlias(alias + "POSTCODE", rs, String.class);
        String county = getValueByAlias(alias + "COUNTY", rs, String.class);
        Region region = Region.valueOf(getValueByAlias(alias + "REGION", rs, String.class).toUpperCase());
        String country = getValueByAlias(alias + "COUNTRY", rs, String.class);
        Address address = new Address(addrId, streetAddress, address2, city, state, postCode, county, region, country);
        return address;
    }

    private <T> T getValueByAlias(String alias, ResultSet rs, Class<T> clazz) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        Integer foundIdx = 0;
        foundIdx = getIndexForAlias(alias, rs, columnCount);
        return foundIdx == 0 ? null : (T) rs.getObject(foundIdx);
    }

    private int getIndexForAlias(String alias, ResultSet rs, int columnCount) throws SQLException {
        Integer foundIdx = aliasColIdxMap.getOrDefault(alias, 0);
        if (foundIdx == 0) {
            for (int columnIdx = 1; columnIdx <= columnCount; columnIdx++) {
                if (alias.equals(rs.getMetaData().getColumnLabel(columnIdx))) {
                    foundIdx = columnIdx;
                    aliasColIdxMap.put(alias, foundIdx);
                    break;
                }
            }
        }
        return foundIdx;
    }


    private static Timestamp convertDobToTimeStamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }

}
