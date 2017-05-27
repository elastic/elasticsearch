/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import java.util.Objects;

import org.elasticsearch.xpack.sql.capabilities.UnresolvedException;

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
    public boolean nullable() {
        throw new UnresolvedException("nullable", this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnresolvedAlias other = (UnresolvedAlias) obj;
        return Objects.equals(child, other.child);
    }

    @Override
    public String toString() {
        return child + " AS ?";
    }
}
