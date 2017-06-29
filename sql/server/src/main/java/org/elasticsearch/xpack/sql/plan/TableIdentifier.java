/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan;

import java.util.Objects;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.tree.Location;

public class TableIdentifier {

    private final String index, type;
    private final Location location;

    public TableIdentifier(Location location, String index, String type) {
        this.location = location;
        this.index = index;
        this.type = type;
    }

    public String index() {
        return index;
    }

    public String type() {
        return type;
    }

    public boolean hasType() {
        return Strings.hasText(type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type);
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
        return Objects.equals(index, other.index) 
                && Objects.equals(type, other.type);
    }

    public Location location() {
        return location;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[index=");
        builder.append(index);
        builder.append(", type=");
        builder.append(type);
        builder.append("]");
        return builder.toString();
    }
}
