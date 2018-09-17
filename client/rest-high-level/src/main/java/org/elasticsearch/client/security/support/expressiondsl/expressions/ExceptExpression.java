/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security.support.expressiondsl.expressions;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A negating expression. That is, this expression evaluates to <code>true</code> if-and-only-if
 * its delegate expression evaluate to <code>false</code>.
 * Syntactically, <em>except</em> expressions are intended to be children of <em>all</em>
 * expressions ({@link AllExpression}).
 */
public final class ExceptExpression implements RoleMapperExpression {

    public static final String NAME = "except";

    private final RoleMapperExpression expression;

    public ExceptExpression(final RoleMapperExpression expression) {
        assert expression != null : "expression cannot be null";
        this.expression = expression;
    }

    public RoleMapperExpression getInnerExpression() {
        return expression;
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

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(NAME);
        expression.toXContent(builder, params);
        return builder.endObject();
    }

}
