/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

public class GeoDistanceQuery extends Query {

    private final String field;
    private final double lat;
    private final double lon;
    private final double distance;

    public GeoDistanceQuery(Source source, String field, double distance, double lat, double lon) {
        super(source);
        this.field = field;
        this.distance = distance;
        this.lat = lat;
        this.lon = lon;
    }

    public String field() {
        return field;
    }

    public double lat() {
        return lat;
    }

    public double lon() {
        return lon;
    }

    public double distance() {
        return distance;
    }

    @Override
    public QueryBuilder asBuilder() {
        return QueryBuilders.geoDistanceQuery(field).distance(distance, DistanceUnit.METERS).point(lat, lon);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, distance, lat, lon);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        GeoDistanceQuery other = (GeoDistanceQuery) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(distance, other.distance)
            && Objects.equals(lat, other.lat)
            && Objects.equals(lon, other.lon);
    }

    @Override
    protected String innerToString() {
        return field + ":" + "(" + distance + "," + "(" + lat + ", " + lon + "))";
    }
}
