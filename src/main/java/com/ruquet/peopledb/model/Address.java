package com.ruquet.peopledb.model;

import com.ruquet.peopledb.anotation.Id;

import java.util.Objects;

public final class Address {
    @Id
    private final Long id;
    private final String streetAddres;
    private final String address2;
    private final String city;
    private final String state;
    private final String postCode;
    private final String county;
    private final Region region;
    private final String country;

    public Address(@Id Long id, String streetAddres, String address2, String city, String state, String postCode, String county,
                   Region region, String country) {
        this.id = id;
        this.streetAddres = streetAddres;
        this.address2 = address2;
        this.city = city;
        this.state = state;
        this.postCode = postCode;
        this.county = county;
        this.region = region;
        this.country = country;
    }

    @Id
    public Long id() {
        return id;
    }

    public String streetAddres() {
        return streetAddres;
    }

    public String address2() {
        return address2;
    }

    public String city() {
        return city;
    }

    public String state() {
        return state;
    }

    public String postCode() {
        return postCode;
    }

    public String county() {
        return county;
    }

    public Region region() {
        return region;
    }

    public String country() {
        return country;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Address) obj;
        return Objects.equals(this.id, that.id) &&
                Objects.equals(this.streetAddres, that.streetAddres) &&
                Objects.equals(this.address2, that.address2) &&
                Objects.equals(this.city, that.city) &&
                Objects.equals(this.state, that.state) &&
                Objects.equals(this.postCode, that.postCode) &&
                Objects.equals(this.county, that.county) &&
                Objects.equals(this.region, that.region) &&
                Objects.equals(this.country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, streetAddres, address2, city, state, postCode, county, region, country);
    }

    @Override
    public String toString() {
        return "Address[" +
                "id=" + id + ", " +
                "streetAddres=" + streetAddres + ", " +
                "address2=" + address2 + ", " +
                "city=" + city + ", " +
                "state=" + state + ", " +
                "postCode=" + postCode + ", " +
                "county=" + county + ", " +
                "region=" + region + ", " +
                "country=" + country + ']';
    }


}


