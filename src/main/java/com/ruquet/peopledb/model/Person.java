package com.ruquet.peopledb.model;

import com.ruquet.peopledb.anotation.Id;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class Person implements Entity {
    @Id
    private Long id;

    private String firstName;
    private String lastName;
    private ZonedDateTime dob;
    private BigDecimal salary = new BigDecimal("0");
    private String email;
    private Optional<Address> homeAddress = Optional.empty();
    private Optional<Address> businessAddress = Optional.empty();
    private Optional<Person> spouse = Optional.empty();
    private Set<Person> children = new HashSet<>();
    private Optional<Person> parent = Optional.empty();


    public Person(Long id, String firstName, String lastName, ZonedDateTime dob, BigDecimal salary) {
        this(id, firstName, lastName, dob);
        this.salary = salary;
    }

    public Person(Long id, String firstName, String lastName, ZonedDateTime dob) {
        this(firstName, lastName, dob);
        this.id = id;
    }

    public Person(String firstName, String lastName, ZonedDateTime dob) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.dob = dob;
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public ZonedDateTime getDob() {
        return dob;
    }

    public void setDob(ZonedDateTime dob) {
        this.dob = dob;
    }

    public BigDecimal getSalary() {
        return salary;
    }

    public void setSalary(BigDecimal salary) {
        this.salary = salary;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Person person)) return false;
        return Objects.equals(id, person.id) && Objects.equals(firstName, person.firstName) && Objects.equals(lastName, person.lastName) && Objects.equals(getDob().withZoneSameInstant(ZoneId.of("UTC")).truncatedTo(ChronoUnit.SECONDS), person.getDob().withZoneSameInstant(ZoneId.of("UTC")).truncatedTo(ChronoUnit.SECONDS));
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, firstName, lastName, dob);
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", dob=" + dob +
                '}';
    }


    public void setHomeAddress(Address homeAddress) {
        this.homeAddress = Optional.ofNullable(homeAddress);
    }

    public Optional<Address> getHomeAddress() {
        return homeAddress;
    }


    public Optional<Address> getBusinessAddress() {
        return businessAddress;
    }

    public void setBusinessAddress(Address businessAddress) {
        this.businessAddress = Optional.ofNullable(businessAddress);
    }

    public void setSpouse(Person spouse) {
        this.spouse = Optional.ofNullable(spouse);
    }

    public Optional<Person> getSpouse() {
        return spouse;
    }

    public void addChild(Person child) {
        children.add(child);
        child.setParent(this);
    }

    public void setParent(Person parent) {
        this.parent = Optional.ofNullable(parent);
    }

    public Optional<Person> getParent() {
        return parent;
    }

    public Set<Person> getChildren() {
        return children;
    }
}
