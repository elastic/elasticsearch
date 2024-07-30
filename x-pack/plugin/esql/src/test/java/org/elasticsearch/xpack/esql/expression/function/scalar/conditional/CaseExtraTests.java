/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

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
                field("first_cond", DataType.BOOLEAN),
                List.of(field("v", DataType.LONG), field("e", DataType.LONG))
            ).children(),
            equalTo(List.of(field("first_cond", DataType.BOOLEAN), field("v", DataType.LONG), field("e", DataType.LONG)))
        );
    }

    public void testElseValueImplied() {
        assertThat(
            new Case(Source.synthetic("case"), field("first_cond", DataType.BOOLEAN), List.of(field("v", DataType.LONG))).children(),
            equalTo(List.of(field("first_cond", DataType.BOOLEAN), field("v", DataType.LONG)))
        );
    }

    public void testPartialFoldDropsFirstFalse() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(field("first", DataType.LONG), field("last_cond", DataType.BOOLEAN), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(),
            equalTo(new Case(Source.synthetic("case"), field("last_cond", DataType.BOOLEAN), List.of(field("last", DataType.LONG))))
        );
    }

    public void testPartialFoldNoop() {
        Case c = new Case(
            Source.synthetic("case"),
            field("first_cond", DataType.BOOLEAN),
            List.of(field("first", DataType.LONG), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), sameInstance(c));
    }

    public void testPartialFoldFirst() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, true, DataType.BOOLEAN),
            List.of(field("first", DataType.LONG), field("last", DataType.LONG))
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), equalTo(field("first", DataType.LONG)));
    }

    public void testPartialFoldFirstAfterKeepingUnknown() {
        Case c = new Case(
            Source.synthetic("case"),
            field("keep_me_cond", DataType.BOOLEAN),
            List.of(
                field("keep_me", DataType.LONG),
                new Literal(Source.EMPTY, true, DataType.BOOLEAN),
                field("first", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(),
            equalTo(
                new Case(
                    Source.synthetic("case"),
                    field("keep_me_cond", DataType.BOOLEAN),
                    List.of(field("keep_me", DataType.LONG), field("first", DataType.LONG))
                )
            )
        );
    }

    public void testPartialFoldSecond() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                field("first", DataType.LONG),
                new Literal(Source.EMPTY, true, DataType.BOOLEAN),
                field("second", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), equalTo(field("second", DataType.LONG)));
    }

    public void testPartialFoldSecondAfterDroppingFalse() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                field("first", DataType.LONG),
                new Literal(Source.EMPTY, true, DataType.BOOLEAN),
                field("second", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), equalTo(field("second", DataType.LONG)));
    }

    public void testPartialFoldLast() {
        Case c = new Case(
            Source.synthetic("case"),
            new Literal(Source.EMPTY, false, DataType.BOOLEAN),
            List.of(
                field("first", DataType.LONG),
                new Literal(Source.EMPTY, false, DataType.BOOLEAN),
                field("second", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(c.partiallyFold(), equalTo(field("last", DataType.LONG)));
    }

    public void testPartialFoldLastAfterKeepingUnknown() {
        Case c = new Case(
            Source.synthetic("case"),
            field("keep_me_cond", DataType.BOOLEAN),
            List.of(
                field("keep_me", DataType.LONG),
                new Literal(Source.EMPTY, false, DataType.BOOLEAN),
                field("first", DataType.LONG),
                field("last", DataType.LONG)
            )
        );
        assertThat(c.foldable(), equalTo(false));
        assertThat(
            c.partiallyFold(),
            equalTo(
                new Case(
                    Source.synthetic("case"),
                    field("keep_me_cond", DataType.BOOLEAN),
                    List.of(field("keep_me", DataType.LONG), field("last", DataType.LONG))
                )
            )
        );
    }
}
