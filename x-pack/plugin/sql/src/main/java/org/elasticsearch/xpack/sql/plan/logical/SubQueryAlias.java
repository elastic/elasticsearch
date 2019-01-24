/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

public class SubQueryAlias extends UnaryPlan {

    private final String alias;

    public SubQueryAlias(Source source, LogicalPlan child, String alias) {
        super(source, child);
        this.alias = alias;
    }

    @Override
    protected NodeInfo<SubQueryAlias> info() {
        return NodeInfo.create(this, SubQueryAlias::new, child(), alias);
    }

    @Override
    protected SubQueryAlias replaceChild(LogicalPlan newChild) {
        return new SubQueryAlias(source(), newChild, alias);
    }

    public String alias() {
        return alias;
    }

    @Override
    public List<Attribute> output() {
        return (alias == null ? child().output() :
                child().output().stream()
                .map(e -> e.withQualifier(alias))
                .collect(toList())
                );
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(alias, super.hashCode());
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
