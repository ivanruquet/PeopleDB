package com.ruquet.peopledb.repository;

import com.ruquet.peopledb.anotation.SQL;
import com.ruquet.peopledb.model.Address;
import com.ruquet.peopledb.model.CrudOperation;
import com.ruquet.peopledb.model.Region;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AdressesRepository extends CrudRepository<Address> {
    public AdressesRepository(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    @SQL(operationType = CrudOperation.FIND_BY_ID, value = """
            SELECT ID, STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY
            FROM ADDRESSES 
            WHERE ID = ?
            """)
    Address extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long id = rs.getLong("ID");
        String streetAddress = rs.getString("STREET_ADDRESS");
        String address2 = rs.getString("ADDRESS2");
        String city = rs.getString("CITY");
        String state = rs.getString("STATE");
        String postCode = rs.getString("POSTCODE");
        String county = rs.getString("COUNTY");
        Region region = Region.valueOf(rs.getString("REGION").toUpperCase());
        String country = rs.getString("COUNTRY");
        Address address = new Address(id, streetAddress, address2, city, state, postCode, county, region, country);
        return address;
    }

    @Override
    @SQL(operationType = CrudOperation.SAVE, value = """
            INSERT INTO ADDRESSES (STREET_ADDRESS, ADDRESS2, CITY, STATE, POSTCODE, COUNTY, REGION, COUNTRY) 
            VALUES(?,?,?,?,?,?,?,?)            
            """)
    void mapForSave(Address entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.streetAddres());
        ps.setString(2, entity.address2());
        ps.setString(3, entity.city());
        ps.setString(4, entity.state());
        ps.setString(5, entity.postCode());
        ps.setString(6, entity.county());
        ps.setString(7, entity.region().toString());
        ps.setString(8, entity.country());
    }

    @Override
    void mapForUpdate(Address entity, PreparedStatement ps) throws SQLException {

    }
}
