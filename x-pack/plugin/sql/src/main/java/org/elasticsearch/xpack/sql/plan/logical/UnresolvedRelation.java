/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.sql.capabilities.Unresolvable;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.plan.TableIdentifier;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class UnresolvedRelation extends LeafPlan implements Unresolvable {

    private final TableIdentifier table;
    private final String alias;
    private final String unresolvedMsg;

    public UnresolvedRelation(Source source, TableIdentifier table, String alias) {
        this(source, table, alias, null);
    }

    public UnresolvedRelation(Source source, TableIdentifier table, String alias, String unresolvedMessage) {
        super(source);
        this.table = table;
        this.alias = alias;
        this.unresolvedMsg = unresolvedMessage == null ? "Unknown index [" + table.index() + "]" : unresolvedMessage;
    }

    @Override
    protected NodeInfo<UnresolvedRelation> info() {
        return NodeInfo.create(this, UnresolvedRelation::new, table, alias, unresolvedMsg);
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
    public String unresolvedMessage() {
        return unresolvedMsg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source(), table, alias, unresolvedMsg);
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
        return source().equals(other.source())
            && table.equals(other.table)
            && Objects.equals(alias, other.alias)
            && unresolvedMsg.equals(other.unresolvedMsg);
    }

    @Override
    public List<Object> nodeProperties() {
        return singletonList(table);
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + table.index();
    }
}
