/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RegexMatch;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.unboundLogicalOptimizerContext;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class ReplaceRegexMatchTests extends ESTestCase {
    private Expression replaceRegexMatch(RegexMatch<?> e) {
        return new ReplaceRegexMatch().rule(e, unboundLogicalOptimizerContext());
    }

    public void testMatchAllWildcardLikeToExist() {
        for (String s : asList("*", "**", "***")) {
            WildcardPattern pattern = new WildcardPattern(s);
            FieldAttribute fa = getFieldAttribute();
            WildcardLike l = new WildcardLike(EMPTY, fa, pattern);
            Expression e = replaceRegexMatch(l);
            assertEquals(IsNotNull.class, e.getClass());
            IsNotNull inn = (IsNotNull) e;
            assertEquals(fa, inn.field());
        }
    }

    public void testMatchAllRLikeToExist() {
        RLikePattern pattern = new RLikePattern(".*");
        FieldAttribute fa = getFieldAttribute();
        RLike l = new RLike(EMPTY, fa, pattern);
        Expression e = replaceRegexMatch(l);
        assertEquals(IsNotNull.class, e.getClass());
        IsNotNull inn = (IsNotNull) e;
        assertEquals(fa, inn.field());
    }

    public void testExactMatchWildcardLike() {
        for (String s : asList("ab", "ab\\*", "ab\\?c")) {
            WildcardPattern pattern = new WildcardPattern(s);
            FieldAttribute fa = getFieldAttribute();
            WildcardLike l = new WildcardLike(EMPTY, fa, pattern);
            Expression e = replaceRegexMatch(l);
            assertEquals(Equals.class, e.getClass());
            Equals eq = (Equals) e;
            assertEquals(fa, eq.left());
            assertEquals(s.replace("\\", StringUtils.EMPTY), eq.right().fold(FoldContext.small()));
        }
    }

    public void testExactMatchRLike() {
        RLikePattern pattern = new RLikePattern("abc");
        FieldAttribute fa = getFieldAttribute();
        RLike l = new RLike(EMPTY, fa, pattern);
        Expression e = replaceRegexMatch(l);
        assertEquals(Equals.class, e.getClass());
        Equals eq = (Equals) e;
        assertEquals(fa, eq.left());
        assertEquals("abc", eq.right().fold(FoldContext.small()));
    }

}
