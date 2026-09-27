/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher.Matcher;

import java.util.List;

/** Guards the distinction between an absent label and a label whose mapping failed resolution. */
public class LabelMatchersTests extends ESTestCase {

    public void testResolutionErrorIsNotAnAbsentLabel() {
        for (String error : List.of("Reference [label] is ambiguous", "Cannot use field [label] with unsupported type")) {
            UnresolvedAttribute label = new UnresolvedAttribute(Source.EMPTY, "label", error);
            Expression predicate = new LabelMatchers(List.of(new LabelMatcher("label", "value", Matcher.EQ))).predicate(
                Source.EMPTY,
                List.of(label),
                EsqlTestUtils.TEST_CFG
            );
            assertFalse("A resolution error must not turn into an empty result", predicate.resolved());
            assertEquals(List.of(label), predicate.collect(UnresolvedAttribute.class));
        }
    }
}
