/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.querydsl.query.Query;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Objects;

public class SpatialRelatesQuery extends Query {
    private final String field;
    private final ShapeRelation queryRelation;
    private final Geometry shape;
    private final DataType dataType;

    public SpatialRelatesQuery(Source source, String field, ShapeRelation queryRelation, Geometry shape, DataType dataType) {
        super(source);
        this.field = field;
        this.queryRelation = queryRelation;
        this.shape = shape;
        this.dataType = dataType;
    }

    @Override
    public boolean containsNestedField(String path, String field) {
        return false;
    }

    @Override
    public Query addNestedField(String path, String field, String format, boolean hasDocValues) {
        return null;
    }

    @Override
    public void enrichNestedSort(NestedSortBuilder sort) {

    }

    @Override
    public QueryBuilder asBuilder() {
        if (EsqlDataTypes.isSpatialGeo(dataType)) {
            GeoShapeQueryBuilder builder = new GeoShapeQueryBuilder(field, shape);
            return builder.relation(queryRelation);
        }
        throw new IllegalArgumentException("Unsupported data type for SpatialRelatesQuery: " + dataType);
    }

    @Override
    protected String innerToString() {
        throw new IllegalArgumentException("SpatialRelatesQuery.innerToString() not implemented");
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, queryRelation, shape, dataType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SpatialRelatesQuery other = (SpatialRelatesQuery) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(queryRelation, other.queryRelation)
            && Objects.equals(shape, other.shape)
            && Objects.equals(dataType, other.dataType);
    }
}
