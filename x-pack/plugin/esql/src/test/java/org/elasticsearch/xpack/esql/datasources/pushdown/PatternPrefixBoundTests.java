/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvInRange;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvRLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;

public class PatternPrefixBoundTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;
    private static final ReferenceAttribute F = new ReferenceAttribute(SRC, "f", DataType.KEYWORD);

    public void testMvLikePrefixGivesHalfOpenRange() {
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("abc*xyz"))), "abc", "abd");
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("abc*"))), "abc", "abd");
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("ab?d*"))), "ab", "ac");
        // An escaped wildcard is part of the literal prefix.
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("a\\*b*"))), "a*b", "a*c");
        // A pattern without wildcards is its own prefix; the range is a superset of the exact match.
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("abc"))), "abc", "abd");
    }

    public void testMvRLikePrefixGivesHalfOpenRange() {
        assertBound(PatternPrefixBound.of(new MvRLike(SRC, F, keyword("abc.*xyz"))), "abc", "abd");
        // Alternation keeps only the prefix common to every branch.
        assertBound(PatternPrefixBound.of(new MvRLike(SRC, F, keyword("EU|EA.*"))), "E", "F");
    }

    public void testTextFieldGetsABound() {
        ReferenceAttribute text = new ReferenceAttribute(SRC, "t", DataType.TEXT);
        assertBound(PatternPrefixBound.of(new MvLike(SRC, text, keyword("abc*"))), text, "abc", "abd");
    }

    public void testUpperBoundSkipsMaximalCodePoints() {
        String maxCp = new String(Character.toChars(Character.MAX_CODE_POINT));
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("a" + maxCp + "*"))), "a" + maxCp, "b");
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("a\uD7FF*"))), "a\uD7FF", "a\uE000");
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("h\u00e9*"))), "h\u00e9", "h\u00ea");
        // No string sorts above a prefix made of maximal code points only, so there is no upper bound and no range.
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, keyword(maxCp + "*"))));
    }

    public void testNoPrefixGivesNoBound() {
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, keyword("*abc"))));
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, keyword("?abc"))));
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, keyword(""))));
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, keyword("*"))));
        assertNull(PatternPrefixBound.of(new MvRLike(SRC, F, keyword(".*abc"))));
        assertNull(PatternPrefixBound.of(new MvRLike(SRC, F, keyword("abc|xyz"))));
        assertNull(PatternPrefixBound.of(new MvRLike(SRC, F, keyword(""))));
    }

    public void testReplacementCharacterInPrefixGivesNoBound() {
        // A stored 0xFF decodes to U+FFFD and matches these patterns, yet its raw bytes lie outside [U+FFFD, U+FFFE).
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, keyword("\uFFFD*"))));
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, keyword("a\uFFFDb*"))));
        assertNull(PatternPrefixBound.of(new MvRLike(SRC, F, keyword("a\uFFFD.*"))));
        // Past the prefix it costs nothing: the bytes the prefix covers are copied verbatim on decode.
        assertBound(PatternPrefixBound.of(new MvLike(SRC, F, keyword("ab*\uFFFD"))), "ab", "ac");
    }

    public void testNonConstantOrInvalidPatternGivesNoBound() {
        ReferenceAttribute p = new ReferenceAttribute(SRC, "p", DataType.KEYWORD);
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, p)));
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, new Literal(SRC, null, DataType.KEYWORD))));
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, new Literal(SRC, List.of(bytes("a*"), bytes("b*")), DataType.KEYWORD))));
        assertNull(PatternPrefixBound.of(new MvLike(SRC, F, keyword("abc\\"))));
        assertNull(PatternPrefixBound.of(new MvRLike(SRC, F, keyword("abc("))));
    }

    public void testNonAttributeFieldGivesNoBound() {
        // Nothing can push a bound over a computed value, so deriving one would only evaluate the field twice.
        assertNull(PatternPrefixBound.of(new MvLike(SRC, new ToLower(SRC, F, TEST_CFG), keyword("abc*"))));
        assertNull(PatternPrefixBound.of(new MvLike(SRC, new Literal(SRC, null, DataType.NULL), keyword("abc*"))));
    }

    public void testOtherExpressionsGiveNoBound() {
        assertNull(PatternPrefixBound.of(new Equals(SRC, F, keyword("abc*"), null)));
    }

    private static void assertBound(MvInRange bound, String lower, String upper) {
        assertBound(bound, F, lower, upper);
    }

    private static void assertBound(MvInRange bound, Expression field, String lower, String upper) {
        assertNotNull(bound);
        assertSame(field, bound.field());
        assertEquals(bytes(lower), bound.lower().fold(FoldContext.small()));
        assertEquals(bytes(upper), bound.upper().fold(FoldContext.small()));
        assertTrue("the lower bound is inclusive", bound.includeLower());
        assertFalse("the upper bound is exclusive", bound.includeUpper());
        assertTrue(bound.resolved());
    }

    private static Literal keyword(String value) {
        return new Literal(SRC, bytes(value), DataType.KEYWORD);
    }

    private static BytesRef bytes(String value) {
        return new BytesRef(value);
    }
}
