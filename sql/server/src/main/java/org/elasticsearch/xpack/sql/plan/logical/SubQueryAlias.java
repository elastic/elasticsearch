/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.tree.Location;

public class SubQueryAlias extends UnaryPlan {

    private final String alias;

    public SubQueryAlias(Location location, LogicalPlan child, String alias) {
        super(location, child);
        this.alias = alias;
    }

    public String alias() {
        return alias;
    }

    @Override
    public List<Attribute> output() {
        return (alias == null ? child().output() : 
                child().output().stream()
                .map(e -> e.withQualifier(alias))
                .collect(Collectors.toList())
                );
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        
        SubQueryAlias other = (SubQueryAlias) obj;
        return Objects.equals(alias, other.alias);
    }
}
