/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class UnresolvedMetadataAttributeExpression extends UnresolvedNamedExpression {

    private final String pattern;

    public UnresolvedMetadataAttributeExpression(Source source, String pattern) {
        super(source, List.of());
        this.pattern = pattern;
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
    public Expression replaceChildren(List<Expression> newChildren) {
        return null;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, UnresolvedMetadataAttributeExpression::new, pattern);
    }

    @Override
    public Attribute toAttribute() {
        throw new UnsupportedOperationException("not supported");
    }

    public String pattern() {
        return pattern;
    }

    @Override
    protected int innerHashCode(boolean ignoreIds) {
        return Objects.hash(super.innerHashCode(ignoreIds), pattern);
    }

    @Override
    protected boolean innerEquals(Object o, boolean ignoreIds) {
        var other = (UnresolvedMetadataAttributeExpression) o;
        return super.innerEquals(other, true) && pattern.equals(other.pattern);
    }

    @Override
    public Nullability nullable() {
        return Nullability.TRUE;
    }

    @Override
    public String unresolvedMessage() {
        return "Unresolved metadata pattern [" + pattern + "]";
    }

    @Override
    public String toString() {
        return UNRESOLVED_PREFIX + pattern;
    }
}
