/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PlanJsonLogTests extends ESTestCase {

    private static EsRelation relation() {
        return new EsRelation(Source.EMPTY, "idx", IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of());
    }

    public void testRenderablePlanGivesItsJson() {
        Filter filter = new Filter(Source.EMPTY, relation(), Literal.TRUE);
        assertThat(PlanJsonLog.toJsonOrNull(filter), equalTo(filter.toJsonString()));
    }

    public void testNoPlanGivesNoJson() {
        assertThat(PlanJsonLog.toJsonOrNull(null), nullValue());
    }

    /**
     * The JSON is part of PROFILE and EXPLAIN, and a query that runs without them must not fail with them because something in its
     * plan cannot be written. The response just has no JSON for that plan.
     */
    public void testAPlanThatCannotBeRenderedGivesNoJsonAndDoesNotThrow() {
        Object unprintable = new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException("cannot be printed");
            }
        };
        Filter filter = new Filter(Source.EMPTY, relation(), new Literal(Source.EMPTY, unprintable, DataType.UNSUPPORTED));

        expectThrows(RuntimeException.class, filter::toJsonString);
        assertThat(PlanJsonLog.toJsonOrNull(filter), nullValue());
    }
}
