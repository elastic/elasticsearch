/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.join.SemiJoin;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;

public class AliasBindingsTests extends ESTestCase {

    public void testBindingsExcludeIndependentSubqueryRightSide() {
        ReferenceAttribute leftInput = new ReferenceAttribute(Source.EMPTY, "left", DataType.INTEGER);
        ReferenceAttribute rightInput = new ReferenceAttribute(Source.EMPTY, "right", DataType.INTEGER);
        Alias leftAlias = new Alias(Source.EMPTY, "value", new Literal(Source.EMPTY, 1, DataType.INTEGER));
        Alias rightAlias = new Alias(Source.EMPTY, "value", new Literal(Source.EMPTY, 2, DataType.INTEGER));
        Eval left = new Eval(Source.EMPTY, relation(leftInput), List.of(leftAlias));
        Eval right = new Eval(Source.EMPTY, relation(rightInput), List.of(rightAlias));
        SemiJoin join = new SemiJoin(Source.EMPTY, left, right, List.of(leftInput), List.of(rightInput));

        var bindings = AliasBindings.of(join);

        assertSame(leftAlias.child(), bindings.resolve(leftAlias.toAttribute(), leftAlias.toAttribute()));
        assertSame(rightAlias.toAttribute(), bindings.resolve(rightAlias.toAttribute(), rightAlias.toAttribute()));
    }

    private static LocalRelation relation(Attribute attribute) {
        return new LocalRelation(Source.EMPTY, List.of(attribute), LocalSupplier.of(new Page(0)));
    }
}
