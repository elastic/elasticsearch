/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import org.elasticsearch.xpack.ql.querydsl.container.Sort;

import java.util.Objects;

public class GroupingFunctionSort extends Sort {

    public GroupingFunctionSort(Direction direction, Missing missing) {
        super(direction, missing);
    }

    @Override
    public int hashCode() {
        return Objects.hash(direction(), missing());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        GroupingFunctionSort other = (GroupingFunctionSort) obj;
        return Objects.equals(direction(), other.direction()) && Objects.equals(missing(), other.missing());
    }
}
