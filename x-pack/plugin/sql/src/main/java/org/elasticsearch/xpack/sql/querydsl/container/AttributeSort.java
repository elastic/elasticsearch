/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Attribute;

public class AttributeSort extends Sort {

    private final Attribute attribute;

    public AttributeSort(Attribute attribute, Direction direction) {
        super(direction);
        this.attribute = attribute;
    }

    public Attribute attribute() {
        return attribute;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attribute, direction());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        AttributeSort other = (AttributeSort) obj;
        return Objects.equals(direction(), other.direction())
                && Objects.equals(attribute, other.attribute);
    }
}
