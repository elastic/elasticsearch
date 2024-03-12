/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.field;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Extra tests for {@code CASE} that don't fit into the parameterized
 * {@link CaseTests}.
 */
public class CaseExtraTests extends ESTestCase {
    public void testElseValueExplicit() {
        assertThat(
            new Case(
                Source.synthetic("case"),
                field("first_cond", DataTypes.BOOLEAN),
                List.of(field("v", DataTypes.LONG), field("e", DataTypes.LONG))
            ).children(),
            equalTo(List.of(field("first_cond", DataTypes.BOOLEAN), field("v", DataTypes.LONG), field("e", DataTypes.LONG)))
        );
    }

    public void testElseValueImplied() {
        assertThat(
            new Case(Source.synthetic("case"), field("first_cond", DataTypes.BOOLEAN), List.of(field("v", DataTypes.LONG))).children(),
            equalTo(List.of(field("first_cond", DataTypes.BOOLEAN), field("v", DataTypes.LONG)))
        );
    }

    public void testPartialFoldDropsFirstFalse() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataTypes.BOOLEAN),
            List.of(field("first", DataTypes.LONG), field("last_cond", DataTypes.BOOLEAN), field("last", DataTypes.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(),
            equalTo(new Case(Source.synthetic("case"), field("last_cond", DataTypes.BOOLEAN), List.of(field("last", DataTypes.LONG))))
        );
    }

    public void testPartialFoldNoop() {
        Case c = new Case(
            Source.synthetic("case"),
            field("first_cond", DataTypes.BOOLEAN),
            List.of(field("first", DataTypes.LONG), field("last", DataTypes.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), sameInstance(c));
    }

    public void testPartialFoldFirst() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, true, DataTypes.BOOLEAN),
            List.of(field("first", DataTypes.LONG), field("last", DataTypes.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), equalTo(field("first", DataTypes.LONG)));
    }

    public void testPartialFoldFirstAfterKeepingUnknown() {
        Case c = new Case(
            Source.synthetic("case"),
            field("keep_me_cond", DataTypes.BOOLEAN),
            List.of(
                field("keep_me", DataTypes.LONG),
                new Literal(Source.EMPTY, true, DataTypes.BOOLEAN),
                field("first", DataTypes.LONG),
                field("last", DataTypes.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(),
            equalTo(
                new Case(
                    Source.synthetic("case"),
                    field("keep_me_cond", DataTypes.BOOLEAN),
                    List.of(field("keep_me", DataTypes.LONG), field("first", DataTypes.LONG))
                )
            )
        );
    }

    public void testPartialFoldSecond() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataTypes.BOOLEAN),
            List.of(
                field("first", DataTypes.LONG),
                new Literal(Source.EMPTY, true, DataTypes.BOOLEAN),
                field("second", DataTypes.LONG),
                field("last", DataTypes.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), equalTo(field("second", DataTypes.LONG)));
    }

    public void testPartialFoldSecondAfterDroppingFalse() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataTypes.BOOLEAN),
            List.of(
                field("first", DataTypes.LONG),
                new Literal(Source.EMPTY, true, DataTypes.BOOLEAN),
                field("second", DataTypes.LONG),
                field("last", DataTypes.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), equalTo(field("second", DataTypes.LONG)));
    }

    public void testPartialFoldLast() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataTypes.BOOLEAN),
            List.of(
                field("first", DataTypes.LONG),
                new Literal(Source.EMPTY, false, DataTypes.BOOLEAN),
                field("second", DataTypes.LONG),
                field("last", DataTypes.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), equalTo(field("last", DataTypes.LONG)));
    }

    public void testPartialFoldLastAfterKeepingUnknown() {
        Case c = new Case(
            Source.synthetic("case"),
            field("keep_me_cond", DataTypes.BOOLEAN),
            List.of(
                field("keep_me", DataTypes.LONG),
                new Literal(Source.EMPTY, false, DataTypes.BOOLEAN),
                field("first", DataTypes.LONG),
                field("last", DataTypes.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(),
            equalTo(
                new Case(
                    Source.synthetic("case"),
                    field("keep_me_cond", DataTypes.BOOLEAN),
                    List.of(field("keep_me", DataTypes.LONG), field("last", DataTypes.LONG))
                )
            )
        );
    }
}
