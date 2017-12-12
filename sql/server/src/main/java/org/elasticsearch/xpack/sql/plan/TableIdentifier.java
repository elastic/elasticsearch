/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan;

import org.elasticsearch.xpack.sql.tree.Location;

import java.util.Objects;

public class TableIdentifier {

    private final String index;
    private final Location location;

    public TableIdentifier(Location location, String index) {
        this.location = location;
        this.index = index;
    }

    public String index() {
        return index;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        TableIdentifier other = (TableIdentifier) obj;
        return Objects.equals(index, other.index);
    }

    public Location location() {
        return location;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[index=");
        builder.append(index);
        builder.append("]");
        return builder.toString();
    }
}
