/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.UnresolvedException;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;

public class UnresolvedStar extends UnresolvedNamedExpression {

    // typically used for nested fields or inner/dotted fields
    private final UnresolvedAttribute qualifier;

    public UnresolvedStar(Source source, UnresolvedAttribute qualifier) {
        super(source, emptyList());
        this.qualifier = qualifier;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("doesn't escape the node");
    }

    @Override
    protected NodeInfo<UnresolvedStar> info() {
        return NodeInfo.create(this, UnresolvedStar::new, qualifier);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new UnsupportedOperationException("this type of node doesn't have any children to replace");
    }

    @Override
    public Nullability nullable() {
        throw new UnresolvedException("nullable", this);
    }

    public UnresolvedAttribute qualifier() {
        return qualifier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(qualifier);
    }

    @Override
    public boolean equals(Object obj) {
        /*
         * Intentionally not calling the superclass
         * equals because it uses id which we always
         * mutate when we make a clone. So we need
         * to ignore it in equals for the transform
         * tests to pass.
         */
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }

        UnresolvedStar other = (UnresolvedStar) obj;
        return Objects.equals(qualifier, other.qualifier);
    }

    private String message() {
        return (qualifier() != null ? qualifier().name() + "." : "") + "*";
    }

    @Override
    public String unresolvedMessage() {
        return "Cannot determine columns for [" + message() + "]";
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
