/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.capabilities.UnresolvedException;

import java.util.Objects;

import static java.util.Collections.singletonList;

public class UnresolvedAlias extends UnresolvedNamedExpression {

    private final Expression child;

    public UnresolvedAlias(Expression child) {
        super(child.location(), singletonList(child));
        this.child = child;
    }

    public Expression child() {
        return child;
    }

    @Override
    public String unresolvedMessage() {
        return "Unknown alias [" + name() + "]";
    }

    @Override
    public boolean nullable() {
        throw new UnresolvedException("nullable", this);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), child);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(child, ((UnresolvedAlias) obj).child);
    }

    @Override
    public String toString() {
        return child + " AS ?";
    }
}
