/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.support.expressiondsl.expressions;

import org.elasticsearch.client.security.support.expressiondsl.RoleMapperExpression;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A negating expression. That is, this expression evaluates to <code>true</code> if-and-only-if
 * its delegate expression evaluate to <code>false</code>.
 * Syntactically, <em>except</em> expressions are intended to be children of <em>all</em>
 * expressions ({@link AllRoleMapperExpression}).
 */
public final class ExceptRoleMapperExpression extends CompositeRoleMapperExpression {

    public ExceptRoleMapperExpression(final RoleMapperExpression expression) {
        super(CompositeType.EXCEPT.getName(), expression);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(CompositeType.EXCEPT.getName());
        builder.value(getElements().get(0));
        return builder.endObject();
    }

}
