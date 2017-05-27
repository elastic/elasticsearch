/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.sql.capabilities.Unresolvable;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.tree.Location;

public class UnresolvedRelation extends LeafPlan implements Unresolvable {

    private final TableIdentifier table;
    private final String alias;

    public UnresolvedRelation(Location location, TableIdentifier table, String alias) {
        super(location);
        this.table = table;
        this.alias = alias;
    }

    public TableIdentifier table() {
        return table;
    }

    public String alias() {
        return alias;
    }

    @Override
    public boolean resolved() {
        return false;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }

    @Override
    public List<Attribute> output() {
        return Collections.emptyList();
    }

    @Override
    public int hashCode() {
        return Objects.hash(table);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        UnresolvedRelation other = (UnresolvedRelation) obj;
        return Objects.equals(table, other.table);
    }
}
