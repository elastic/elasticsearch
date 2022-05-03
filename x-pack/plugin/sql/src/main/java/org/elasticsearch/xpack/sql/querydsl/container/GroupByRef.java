/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.ql.execution.search.AggRef;
import org.elasticsearch.xpack.ql.type.DataType;

/**
 * Reference to a GROUP BY agg (typically this gets translated to a composite key).
 */
public class GroupByRef extends AggRef {

    public enum Property {
        VALUE,
        COUNT;
    }

    private final String key;
    private final Property property;
    private final DataType dataType;

    public GroupByRef(String key, Property property, DataType dataType) {
        this.key = key;
        this.property = property == null ? Property.VALUE : property;
        this.dataType = dataType;
    }

    public String key() {
        return key;
    }

    public Property property() {
        return property;
    }

    public DataType dataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return "|" + key + (property == Property.COUNT ? ".count" : "") + "|";
    }
}
