/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinConfig;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;

import java.util.List;

public class JoinTests extends ESTestCase {
    public void testReferences() {
        List<Alias> leftAliases = List.of(aliasForLiteral("left1"), aliasForLiteral("left2"), aliasForLiteral("left3"));
        List<Alias> rightAliases = List.of(aliasForLiteral("right1"), aliasForLiteral("right2"), aliasForLiteral("right3"));
        Row left = new Row(Source.EMPTY, leftAliases);
        Row right = new Row(Source.EMPTY, rightAliases);

        List<NamedExpression> matchFields = List.of(leftAliases.get(0).toAttribute());
        List<Expression> conditions = List.of(
            new Equals(Source.EMPTY, leftAliases.get(0).toAttribute(), rightAliases.get(0).toAttribute())
        );
        JoinConfig joinConfig = new JoinConfig(JoinType.LEFT, matchFields, conditions);
        Join join = new Join(Source.EMPTY, left, right, joinConfig);

        assertEquals(2, join.references().size());
        assertTrue(join.references().containsAll(List.of(leftAliases.get(0).toAttribute(), rightAliases.get(0).toAttribute())));
    }

    private static Alias aliasForLiteral(String name) {
        return new Alias(Source.EMPTY, name, new Literal(Source.EMPTY, 1, DataType.INTEGER));
    }
}
