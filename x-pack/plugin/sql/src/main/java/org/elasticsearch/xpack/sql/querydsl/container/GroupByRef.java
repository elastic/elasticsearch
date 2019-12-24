/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.sql.execution.search.AggRef;

/**
 * Reference to a GROUP BY agg (typically this gets translated to a composite key).
 */
public class GroupByRef extends AggRef {

    public enum Property {
        VALUE, COUNT;
    }
    
    private final String key;
    private final Property property;
    private final boolean isDateTimeBased;

    public GroupByRef(String key, Property property, boolean isDateTimeBased) {
        this.key = key;
        this.property = property == null ? Property.VALUE : property;
        this.isDateTimeBased = isDateTimeBased;
    }

    public String key() {
        return key;
    }

    public Property property() {
        return property;
    }

    public boolean isDateTimeBased() {
        return isDateTimeBased;
    }

    @Override
    public String toString() {
        return "|" + key + (property == Property.COUNT ? ".count" : "") + "|";
    }
}
