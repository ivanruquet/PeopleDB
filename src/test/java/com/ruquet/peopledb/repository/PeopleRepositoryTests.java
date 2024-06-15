package com.ruquet.peopledb.repository;

import com.ruquet.peopledb.model.Address;
import com.ruquet.peopledb.model.Person;
import com.ruquet.peopledb.model.Region;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private Connection connection;
    private PeopleRepository repo;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:C:\\Users\\Ivan\\Documents\\Data Base\\peopletest");
        connection.setAutoCommit(false); //es un parametro, para que no se guarden automaticamente los test de Person
        repo = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        Address address = new Address(null, "Horacio Quiroga, 4864", "Mansilla", "Ituzaingo", "BA", "1714", "Provincia", Region.WEST, "Argentina");
        ivan.setHomeAddress(address);
        Person savedPerson = repo.save(ivan);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSave2Person() {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        Person jorge = new Person("Jorge", "Pinarello", ZonedDateTime.of(1982, 02, 12, 02, 22, 26, 55, ZoneId.of("-3")));
        Person savedPerson1 = repo.save(ivan);
        Person savedPerson2 = repo.save(jorge);
        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void candSavePersonWithAddress() throws SQLException {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        Address address = new Address(null, "Horacio Quisssssga, 4864", "Mansilla", "Ituzaingo", "BA", "1714", "Provincia", Region.WEST, "Argentina");
        ivan.setHomeAddress(address);
        Person savedPerson = repo.save(ivan);
        assertThat(savedPerson.getHomeAddress().get().state()).isEqualTo("BA");
    }

    @Test
    public void canSavePersonWithAnotherAddress() throws SQLException {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        Address address = new Address(null, "Horacio Quiroga, 4864", "Mansilla", "Ituzaingo", "BA", "1714", "Provincia", Region.WEST, "Argentina");
        ivan.setBusinessAddress(address);
        Person savedPerson = repo.save(ivan);
        assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0l);
    }

    @Test
    public void canSavePersonWithSpouse() throws SQLException {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        Person spouse = new Person("Ivana", "Ruqueta", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        ivan.setSpouse(spouse);
        Person savedPerson = repo.save(ivan);
        assertThat(savedPerson.getSpouse().get().getFirstName()).isEqualTo("Ivana");
    }

    @Test
    public void canSavePersonWithChildren() throws SQLException {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        ivan.addChild(new Person("Pepo", "Ruquet", ZonedDateTime.of(2020, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        ivan.addChild(new Person("Tute", "Ruquet", ZonedDateTime.of(2022, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        ivan.addChild(new Person("Menem", "Ruquet", ZonedDateTime.of(2024, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person savedPerson = repo.save(ivan);
        savedPerson.getChildren().stream().map(Person::getId)
                .forEach(id -> assertThat(id).isGreaterThan(0));
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = repo.save(new Person("asd", "asd", ZonedDateTime.of(1994, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindPersonWithChildrensById() {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        ivan.addChild(new Person("Pepo", "Ruquet", ZonedDateTime.of(2020, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        ivan.addChild(new Person("Tute", "Ruquet", ZonedDateTime.of(2022, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        ivan.addChild(new Person("Menem", "Ruquet", ZonedDateTime.of(2024, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person savedPerson = repo.save(ivan);
        Person person = repo.findById(savedPerson.getId()).get();
        assertThat(person.getChildren().stream().map(Person::getFirstName).collect(toSet())).contains("Pepo","Tute","Menem");
    }

    @Test
    public void canFindPersonByIdWithHomeAddress() throws SQLException {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        Address address = new Address(null, "Horacio Quiroga, 4864", "Mansilla", "Ituzaingo", "BA", "1714", "Provincia", Region.WEST, "Argentina");
        ivan.setHomeAddress(address);
        Person spouse = new Person("Ivana", "Ruqueta", ZonedDateTime.of(2000, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        ivan.setSpouse(spouse);
        Person savedPerson = repo.save(ivan);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getHomeAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canFindPersonByIdWithBizzAddress() throws SQLException {
        Person ivan = new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3")));
        Address address = new Address(null, "Horacio Quiroga, 4864", "Mansilla", "Ituzaingo", "BA", "1714", "Provincia", Region.WEST, "Argentina");
        ivan.setBusinessAddress(address);
        Person savedPerson = repo.save(ivan);
        Person foundPerson = repo.findById(savedPerson.getId()).get();
        assertThat(foundPerson.getBusinessAddress().get().state()).isEqualTo("BA");
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canGetCount() {
        long count = 0;
        Person savedPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person s2avedPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person sa2vedPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person sav3edPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person sav23edPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Long countFinal = repo.getCount();
        assertThat(countFinal).isLessThan(6000000L);
    }

    @Test
    public void canDelete() {
        Person savedPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        long count = repo.getCount();
        repo.delete(savedPerson);
        Long finalCount = repo.getCount();
        assertThat(finalCount).isEqualTo(count - 1);

    }

    @Test
    public void canDeleteMultiPeople() {
        Person savedPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person savedPerson2 = repo.save(new Person("Ivan123123", "Ruquet123", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        long count = repo.getCount();
        repo.delete(savedPerson, savedPerson2);
        Long finalCount = repo.getCount();
        assertThat(finalCount).isEqualTo(count - 2);
    }
//    @Test
//    public void experiment(){
//        Person p1 = new Person(10L, null,null,null);
//        Person p2 = new Person(20L, null,null,null);
//        Person p3 = new Person(30L, null,null,null);
//        Person p4 = new Person(40L, null,null,null);
//        Person p5 = new Person(50L, null,null,null);
//        Person[] people = Arrays.asList(p1, p2, p3, p4, p5).toArray(new Person[]{});
//        String ids = Arrays.stream(people)
//                .map(s -> s.getId())
//                .map(String::valueOf)
//                .collect(Collectors.joining(","));
//
//    }

    @Test
    public void canFindAll() {
        Person savedPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person savedPerson2 = repo.save(new Person("Jenny", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        List<Person> people = repo.findAll();
        assertThat(people.size()).isGreaterThan(10);
    }


    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("Ivan", "Ruquet", ZonedDateTime.of(1996, 06, 22, 02, 22, 26, 55, ZoneId.of("-3"))));
        Person p1 = repo.findById(savedPerson.getId()).get();
        savedPerson.setSalary(new BigDecimal("73000.0"));
        repo.update(savedPerson);
        Person p2 = repo.findById(savedPerson.getId()).get();
        assertThat(p1.getSalary()).isNotEqualTo(p2.getSalary());


    }

    @Test
    @Disabled
    public void loadData() throws IOException, SQLException {
        Files.lines(Path.of("C://Users//Ivan//Desktop//Hr5m//Hr5m.csv"))
                .skip(1)
                .map(s -> s.split(","))
                .map(s -> {
                    LocalDate dob = LocalDate.parse(s[10], DateTimeFormatter.ofPattern("M/d/yyyy"));
                    LocalTime tob = LocalTime.parse(s[11], DateTimeFormatter.ofPattern("hh:mm:ss a").withLocale(Locale.US));
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
                    Person person = new Person(s[2], s[4], zdtob);
                    person.setSalary(new BigDecimal(s[25]));
                    person.setEmail(s[6]);
                    return person;
                })
                .forEach(repo::save);
        connection.commit();
    }


}
