package com.ruquet.peopledb.model;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class PersonTest {

    @Test
    public void testForEquality(){
        Person p1 = new Person("Ivan", "Ruquet", ZonedDateTime.now());
        Person p2 = new Person("Ivan", "Ruquet", ZonedDateTime.now());
        assertThat(p1).isEqualTo(p2);
    }
}