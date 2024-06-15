package com.ruquet.peopledb.repository;

import com.ruquet.peopledb.anotation.Id;
import com.ruquet.peopledb.anotation.MultiSQL;
import com.ruquet.peopledb.anotation.SQL;
import com.ruquet.peopledb.com.ruquet.peopledb.exceptions.UnableToSave;
import com.ruquet.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

abstract class CrudRepository<T> {

    protected Connection connection;
    private PreparedStatement savePS;
    private PreparedStatement findByIdPS;


    public CrudRepository(Connection connection) throws SQLException {
        this.connection = connection;
        savePS = connection.prepareStatement(getSqlByAnotation(CrudOperation.SAVE, this::getSaveSQL), Statement.RETURN_GENERATED_KEYS);
        findByIdPS = connection.prepareStatement(getSqlByAnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSql));
    }

    private String getSqlByAnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(ms -> ms.isAnnotationPresent(MultiSQL.class))
                .map(ms -> ms.getAnnotation(MultiSQL.class))
                .flatMap(ms -> Arrays.stream(ms.value()));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(s -> s.isAnnotationPresent(SQL.class))
                .map(s -> s.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
                .filter(a -> a.operationType().equals(operationType))
                .map(s -> s.value())
                .findFirst().orElseGet(sqlGetter);
    }

    public T save(T entity) throws UnableToSave {
        Long id = null;
        try {
            savePS.clearParameters();
            mapForSave(entity, savePS);
            int recordsAffected = savePS.executeUpdate();
            System.out.printf("Records Affected: %d%n", recordsAffected);
            ResultSet rs = savePS.getGeneratedKeys();
            while (rs.next()) {
                 id = rs.getLong(1);
                setByAnnotation(id, entity);

                //System.out.println(entity);
            }
        } catch (SQLException e) {
            throw new UnableToSave("Tried to save person: " + entity);
        }
        postSave(entity, id);
        return entity;
    }

    protected void postSave(T entity, long id) {
    }

    public void update(T entity) {
        try {
            PreparedStatement updatePS = connection.prepareStatement(getSqlByAnotation(CrudOperation.UPDATE, this::getUpdateSql));
            mapForUpdate(entity, updatePS);
            updatePS.setLong(5, getByAnnotation(entity));
            updatePS.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void setByAnnotation(Long id, T entity) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .forEach(f -> {
                    f.setAccessible(true);
                    try {
                        f.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to save an Id");
                    }
                });
    }

    private Long getByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(f -> f.isAnnotationPresent(Id.class))
                .map(f -> {
                    f.setAccessible(true);
                    Long id = 0L;
                    try {
                        id = (Long) f.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    return id;
                })
                .findFirst().orElseThrow(() -> new RuntimeException("No ID Found"));
    }


    public Optional<T> findById(Long id) {
        T entity = null;
        try {
            findByIdPS.setLong(1, id);
            ResultSet rs = findByIdPS.executeQuery();
            while (rs.next()) {
                entity = extractEntityFromResultSet(rs);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(entity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();
        try {
            PreparedStatement findAllPS = connection.prepareStatement(getSqlByAnotation(CrudOperation.FIND_ALL, this::getFindAllSql), ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            ResultSet rs = findAllPS.executeQuery();
            while (rs.next()) {
                entities.add(extractEntityFromResultSet(rs));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return entities;
    }

    public Long getCount() {
        Long count = 0L;
        try {
            PreparedStatement getCountPS = connection.prepareStatement(getSqlByAnotation(CrudOperation.COUNT, this::getCountSql));
            ResultSet resultSet = getCountPS.executeQuery();
            while (resultSet.next()) {
                count = resultSet.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return count;
    }

    public void delete(T entity) {
        PreparedStatement deletePS = null;
        try {
             deletePS = connection.prepareStatement(getSqlByAnotation(CrudOperation.DELETE_ONE, this::getDeleteSql));
            deletePS.setLong(1, getByAnnotation(entity));
            int recordsAffected = deletePS.executeUpdate();
            System.out.println(recordsAffected);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void delete(T... entities) {
        try {
            Statement stmt = connection.createStatement();
            String ids = Arrays.stream(entities).map(this::getByAnnotation).map(String::valueOf).collect(joining(","));
            int recordsAffected = stmt.executeUpdate(getSqlByAnotation(CrudOperation.DELETE_MANY, this::getDeleteManyInSql).replace(":ids", ids));
            System.out.println(recordsAffected);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    protected String getUpdateSql() {
        throw new RuntimeException("Can't find sql input");

    }

    protected String getDeleteManyInSql() {
        throw new RuntimeException("Can't find sql input");

    }

    /**
     * @return Should return a SQL string like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter and call it 'ids'
     */
    protected String getDeleteSql() {
        throw new RuntimeException("Can't find sql input");

    }

    protected String getCountSql() {
        throw new RuntimeException("Can't find sql input");

    }

    protected String getFindAllSql() {
        throw new RuntimeException("Can't find sql input");

    }

    /**
     * @return Return a String that represent the SQL needed to retrieve an entity,
     * the sql must contain a sql parameter, i.e "?", that will blind to the entity's id.
     */
    protected String getFindByIdSql() {
        throw new RuntimeException("Can't find sql input");

    }

    private String getSaveSQL() {
        throw new RuntimeException("Can't find sql input");
    }

    abstract T extractEntityFromResultSet(ResultSet rs) throws SQLException;

    abstract void mapForSave(T entity, PreparedStatement ps) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement ps) throws SQLException;
}
