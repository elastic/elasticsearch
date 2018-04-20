/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * A negating expression. That is, this expression evaluates to <code>true</code> if-and-only-if
 * its delegate expression evaluate to <code>false</code>.
 * Syntactically, <em>except</em> expressions are intended to be children of <em>all</em>
 * expressions ({@link AllExpression}).
 */
public final class ExceptExpression implements RoleMapperExpression {

    public static final String NAME = "except";

    private final RoleMapperExpression expression;

    ExceptExpression(RoleMapperExpression expression) {
        assert expression != null;
        this.expression = expression;
    }

    public ExceptExpression(StreamInput in) throws IOException {
        this(ExpressionParser.readExpression(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ExpressionParser.writeExpression(expression, out);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean match(ExpressionModel model) {
        return !expression.match(model);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ExceptExpression that = (ExceptExpression) o;
        return this.expression.equals(that.expression);
    }

    @Override
    public int hashCode() {
        return expression.hashCode();
    }

    RoleMapperExpression getInnerExpression() {
        return expression;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ExpressionParser.Fields.EXCEPT.getPreferredName());
        expression.toXContent(builder, params);
        return builder.endObject();
    }
}
