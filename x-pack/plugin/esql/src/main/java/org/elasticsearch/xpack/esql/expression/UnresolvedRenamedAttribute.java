/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.ql.capabilities.UnresolvedException;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedNamedExpression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class UnresolvedRenamedAttribute extends UnresolvedNamedExpression {

    private final UnresolvedAttribute newName;
    private final UnresolvedAttribute oldName;

    public UnresolvedRenamedAttribute(Source source, UnresolvedAttribute newName, UnresolvedAttribute oldName) {
        super(source, emptyList());
        this.newName = newName;
        this.oldName = oldName;
    }

    @Override
    protected NodeInfo<UnresolvedRenamedAttribute> info() {
        return NodeInfo.create(this, UnresolvedRenamedAttribute::new, newName, oldName);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    public Nullability nullable() {
        throw new UnresolvedException("nullable", this);
    }

    public UnresolvedAttribute newName() {
        return newName;
    }

    public UnresolvedAttribute oldName() {
        return oldName;
    }

    @Override
    public int hashCode() {
        return Objects.hash(newName, oldName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        UnresolvedRenamedAttribute other = (UnresolvedRenamedAttribute) obj;
        return Objects.equals(newName, other.newName) && Objects.equals(oldName, other.oldName);
    }

    private String message() {
        return "(" + newName() + "," + oldName() + ")";
    }

    @Override
    public String unresolvedMessage() {
        return "Cannot resolve " + message() + "";
    }

    @Override
    public String nodeString() {
        return toString();
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + message();
    }
}
