/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.execution.search.AggRef;

import java.time.ZoneId;

/**
 * Reference to a GROUP BY agg (typically this gets translated to a composite key).
 */
public class GroupByRef extends AggRef {

    public enum Property {
        VALUE, COUNT;
    }
    
    private final String key;
    private final Property property;
    private final ZoneId zoneId;

    public GroupByRef(String key, Property property, ZoneId zoneId) {
        this.key = key;
        this.property = property == null ? Property.VALUE : property;
        this.zoneId = zoneId;
    }

    public String key() {
        return key;
    }

    public Property property() {
        return property;
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    @Override
    public String toString() {
        return "|" + key + (property == Property.COUNT ? ".count" : "") + "|";
    }
}
