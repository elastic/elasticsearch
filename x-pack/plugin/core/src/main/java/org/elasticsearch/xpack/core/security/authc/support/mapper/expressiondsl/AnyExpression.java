/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * An expression that evaluates to <code>true</code> if at least one of its children
 * evaluate to <code>true</code>.
 * An <em>any</em> expression with no children is never <code>true</code>.
 */
public final class AnyExpression implements RoleMapperExpression {

    public static final String NAME = "any";

    private final List<RoleMapperExpression> elements;

    AnyExpression(List<RoleMapperExpression> elements) {
        assert elements != null;
        this.elements = elements;
    }

    public AnyExpression(StreamInput in) throws IOException {
        this(ExpressionParser.readExpressionList(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExpressionParser.writeExpressionList(elements, out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean match(ExpressionModel model) {
        return elements.stream().anyMatch(RoleMapperExpression.predicate(model));
    }

    public List<RoleMapperExpression> getElements() {
        return Collections.unmodifiableList(elements);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final AnyExpression that = (AnyExpression) o;
        return this.elements.equals(that.elements);
    }

    @Override
    public int hashCode() {
        return elements.hashCode();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(ExpressionParser.Fields.ANY.getPreferredName());
        for (RoleMapperExpression e : elements) {
            e.toXContent(builder, params);
        }
        builder.endArray();
        return builder.endObject();
    }

}
