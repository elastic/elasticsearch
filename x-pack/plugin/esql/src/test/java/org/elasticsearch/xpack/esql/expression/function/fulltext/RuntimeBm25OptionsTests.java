/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25Options.RuntimeSimilarity;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.fulltext.RuntimeBm25Options.SIMILARITY_FUNCTION_OPTION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class RuntimeBm25OptionsTests extends ESTestCase {

    /** Release builds do not know the key, so the existing unknown-option message rejects it. */
    public void testReleaseBuildsDoNotAcceptTheKey() {
        assertThat(RuntimeBm25Options.allowedOptions(Match.ALLOWED_OPTIONS, false), sameInstance(Match.ALLOWED_OPTIONS));
        assertThat(RuntimeBm25Options.allowedOptions(MatchPhrase.ALLOWED_OPTIONS, false), sameInstance(MatchPhrase.ALLOWED_OPTIONS));
    }

    public void testSnapshotBuildsAcceptTheKey() {
        Map<String, DataType> allowed = RuntimeBm25Options.allowedOptions(Match.ALLOWED_OPTIONS, true);
        assertThat(allowed, hasEntry(SIMILARITY_FUNCTION_OPTION, DataType.KEYWORD));
        Match.ALLOWED_OPTIONS.forEach((key, type) -> assertThat(allowed, hasEntry(key, type)));
        assertThat(allowed.size(), equalTo(Match.ALLOWED_OPTIONS.size() + 1));
    }

    public void testSimilarityIsCaseInsensitiveAndDefaultsToBoolean() {
        assertThat(RuntimeBm25Options.similarity(null), equalTo(RuntimeSimilarity.BOOLEAN));
        assertThat(RuntimeBm25Options.similarity(options("boost", 2.0f)), equalTo(RuntimeSimilarity.BOOLEAN));
        assertThat(RuntimeBm25Options.similarity(options(SIMILARITY_FUNCTION_OPTION, "BM25")), equalTo(RuntimeSimilarity.BM25));
        assertThat(RuntimeBm25Options.similarity(options(SIMILARITY_FUNCTION_OPTION, "bm25")), equalTo(RuntimeSimilarity.BM25));
        assertThat(RuntimeBm25Options.similarity(options(SIMILARITY_FUNCTION_OPTION, "Boolean")), equalTo(RuntimeSimilarity.BOOLEAN));
    }

    public void testResolveValueRejectsOtherValues() {
        Expression.TypeResolution resolution = RuntimeBm25Options.resolveValue(options(SIMILARITY_FUNCTION_OPTION, "bm42"), "MATCH(t, q)");
        assertTrue(resolution.unresolved());
        assertThat(resolution.message(), equalTo("Invalid option [similarity_function] in [MATCH(t, q)], expected one of [bm25, boolean]"));
        assertTrue(RuntimeBm25Options.resolveValue(options(SIMILARITY_FUNCTION_OPTION, "bm25"), "MATCH(t, q)").resolved());
        assertTrue(RuntimeBm25Options.resolveValue(null, "MATCH(t, q)").resolved());
    }

    /** An option map holding only {@code similarity_function} takes the path the same query would take with no options. */
    public void testHasQueryBuilderOptions() {
        assertFalse(RuntimeBm25Options.hasQueryBuilderOptions(null));
        assertFalse(RuntimeBm25Options.hasQueryBuilderOptions(options(SIMILARITY_FUNCTION_OPTION, "bm25")));
        assertTrue(RuntimeBm25Options.hasQueryBuilderOptions(options(SIMILARITY_FUNCTION_OPTION, "bm25", "boost", 2.0f)));
        assertTrue(RuntimeBm25Options.hasQueryBuilderOptions(options("boost", 2.0f)));
    }

    public void testQueryBuilderOptionsDropTheKey() {
        Map<String, Object> withKey = Map.of(SIMILARITY_FUNCTION_OPTION, "bm25", "boost", 2.0f);
        assertThat(RuntimeBm25Options.queryBuilderOptions(withKey), not(hasEntry(SIMILARITY_FUNCTION_OPTION, (Object) "bm25")));
        assertThat(RuntimeBm25Options.queryBuilderOptions(withKey), hasEntry("boost", (Object) 2.0f));
        Map<String, Object> withoutKey = Map.of("boost", 2.0f);
        assertThat(RuntimeBm25Options.queryBuilderOptions(withoutKey), sameInstance(withoutKey));
    }

    public void testIsSetWhateverTheValue() {
        assertFalse(RuntimeBm25Options.isSet(null));
        assertFalse(RuntimeBm25Options.isSet(options("boost", 2.0f)));
        assertTrue(RuntimeBm25Options.isSet(options(SIMILARITY_FUNCTION_OPTION, "boolean")));
        assertTrue(RuntimeBm25Options.isSet(options(SIMILARITY_FUNCTION_OPTION, "bm25")));
    }

    private static MapExpression options(Object... keysAndValues) {
        List<Expression> entries = new ArrayList<>(keysAndValues.length);
        for (int i = 0; i < keysAndValues.length; i += 2) {
            entries.add(Literal.keyword(Source.EMPTY, (String) keysAndValues[i]));
            Object value = keysAndValues[i + 1];
            entries.add(
                value instanceof Float f ? new Literal(Source.EMPTY, f, DataType.FLOAT) : Literal.keyword(Source.EMPTY, (String) value)
            );
        }
        return new MapExpression(Source.EMPTY, entries);
    }
}
