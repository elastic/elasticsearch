/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.container;

import java.util.Objects;

public class GroupingFunctionSort extends Sort {

    private final String id;

    public GroupingFunctionSort(String id, Direction direction, Missing missing) {
        super(direction, missing);
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(direction(), missing(), id);
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
        return Objects.equals(direction(), other.direction())
                && Objects.equals(missing(), other.missing())
                && Objects.equals(id(), other.id());
    }
}
