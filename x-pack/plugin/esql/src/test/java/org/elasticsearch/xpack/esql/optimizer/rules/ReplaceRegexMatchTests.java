/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Like;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.LikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLike;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardLike;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class ReplaceRegexMatchTests extends ESTestCase {

    public void testMatchAllLikeToExist() {
        for (String s : asList("%", "%%", "%%%")) {
            LikePattern pattern = new LikePattern(s, (char) 0);
            FieldAttribute fa = getFieldAttribute();
            Like l = new Like(EMPTY, fa, pattern);
            Expression e = new ReplaceRegexMatch().rule(l);
            assertEquals(IsNotNull.class, e.getClass());
            IsNotNull inn = (IsNotNull) e;
            assertEquals(fa, inn.field());
        }
    }

    public void testMatchAllWildcardLikeToExist() {
        for (String s : asList("*", "**", "***")) {
            WildcardPattern pattern = new WildcardPattern(s);
            FieldAttribute fa = getFieldAttribute();
            WildcardLike l = new WildcardLike(EMPTY, fa, pattern);
            Expression e = new ReplaceRegexMatch().rule(l);
            assertEquals(IsNotNull.class, e.getClass());
            IsNotNull inn = (IsNotNull) e;
            assertEquals(fa, inn.field());
        }
    }

    public void testMatchAllRLikeToExist() {
        RLikePattern pattern = new RLikePattern(".*");
        FieldAttribute fa = getFieldAttribute();
        RLike l = new RLike(EMPTY, fa, pattern);
        Expression e = new ReplaceRegexMatch().rule(l);
        assertEquals(IsNotNull.class, e.getClass());
        IsNotNull inn = (IsNotNull) e;
        assertEquals(fa, inn.field());
    }

    public void testExactMatchLike() {
        for (String s : asList("ab", "ab0%", "ab0_c")) {
            LikePattern pattern = new LikePattern(s, '0');
            FieldAttribute fa = getFieldAttribute();
            Like l = new Like(EMPTY, fa, pattern);
            Expression e = new ReplaceRegexMatch().rule(l);
            assertEquals(Equals.class, e.getClass());
            Equals eq = (Equals) e;
            assertEquals(fa, eq.left());
            assertEquals(s.replace("0", StringUtils.EMPTY), eq.right().fold());
        }
    }

    public void testExactMatchWildcardLike() {
        String s = "ab";
        WildcardPattern pattern = new WildcardPattern(s);
        FieldAttribute fa = getFieldAttribute();
        WildcardLike l = new WildcardLike(EMPTY, fa, pattern);
        Expression e = new ReplaceRegexMatch().rule(l);
        assertEquals(Equals.class, e.getClass());
        Equals eq = (Equals) e;
        assertEquals(fa, eq.left());
        assertEquals(s, eq.right().fold());
    }

    public void testExactMatchRLike() {
        RLikePattern pattern = new RLikePattern("abc");
        FieldAttribute fa = getFieldAttribute();
        RLike l = new RLike(EMPTY, fa, pattern);
        Expression e = new ReplaceRegexMatch().rule(l);
        assertEquals(Equals.class, e.getClass());
        Equals eq = (Equals) e;
        assertEquals(fa, eq.left());
        assertEquals("abc", eq.right().fold());
    }

}
