/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2Pattern;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.Re2Like;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * A node that predates {@link Re2Like} can't read it, so while one is in the cluster the expression falls back to
 * {@link RLike} over the same pattern, if Lucene's syntax reads it as RE2 does.
 */
public class Re2LikeTransportVersionTests extends ESTestCase {

    private static final TransportVersion ESQL_RE2_LIKE = TransportVersion.fromName("esql_re2_like");

    public void testSupported() {
        Re2Like re2Like = re2Like();
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(ESQL_RE2_LIKE);
        assertThat(re2Like.forTransportVersion(version), sameInstance(re2Like));
    }

    public void testFallsBackToRLike() {
        Re2Like re2Like = re2Like();
        TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(ESQL_RE2_LIKE);
        Expression replaced = re2Like.forTransportVersion(version);
        RLike rLike = as(replaced, RLike.class);
        assertThat(rLike.field(), equalTo(re2Like.field()));
        assertThat(rLike.pattern().pattern(), equalTo(re2Like.pattern().pattern()));
        assertFalse(rLike.caseInsensitive());
    }

    /**
     * Lucene's syntax gives these runes meanings that RE2 doesn't ({@code ~ & @ < > "}) or can't parse RE2's groups, so
     * matching them on an older node would select other series, or fail there.
     */
    public void testRefusesPatternTheSyntaxesReadDifferently() {
        String regex = randomFrom("a~b", "a&b", "a@", "<1-5>", "\"a\"", "(?:a|b)c");
        Re2Like re2Like = new Re2Like(Source.EMPTY, getFieldAttribute("label"), new Re2Pattern(regex));
        TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(ESQL_RE2_LIKE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> re2Like.forTransportVersion(version));
        assertThat(e.getMessage(), containsString("[" + regex + "] can't be matched until every node is upgraded"));
    }

    private static Re2Like re2Like() {
        return new Re2Like(Source.EMPTY, getFieldAttribute("label"), new Re2Pattern(randomFrom("a.*", "foo|bar", "[a-z]+_total")));
    }
}
