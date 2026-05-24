/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
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

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        if (mapper == NodeStringMapper.IDENTITY) {
            super.nodeString(sb, format, mapper);
            return;
        }
        sb.append(UNRESOLVED_PREFIX);
        // Local wildcard parser — same shape as UnresolvedNamePattern.rewriteWildcardPattern but
        // duplicated here because this class lives in core.expression and can't reach into
        // xpack.esql.expression without breaking the layering.
        if (pattern == null || pattern.isEmpty()) {
            return;
        }
        StringBuilder run = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < pattern.length()) {
                run.append(pattern.charAt(++i));
                continue;
            }
            if (c == '*' || c == '?' || c == '%' || c == '_') {
                if (run.length() > 0) {
                    sb.append(mapper.column(run.toString()));
                    run.setLength(0);
                }
                sb.append(c);
            } else {
                run.append(c);
            }
        }
        if (run.length() > 0) {
            sb.append(mapper.column(run.toString()));
        }
    }
}
