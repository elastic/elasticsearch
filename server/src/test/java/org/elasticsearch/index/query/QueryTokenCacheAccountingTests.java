/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.TrackingCircuitBreaker;
import org.elasticsearch.test.AbstractBuilderTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Every query that buffers its whole analyzed text before building clauses charges that buffer to the request breaker, and
 * releases it as soon as the query is built rather than at the end of the request.
 */
public class QueryTokenCacheAccountingTests extends AbstractBuilderTestCase {

    /**
     * Queries keyed by name, built from a text that the standard analyzer splits into many tokens. Commas are not query
     * syntax and do not join letters, so query_string and simple_query_string hand the whole text to one analysis.
     */
    private static Map<String, Function<String, QueryBuilder>> queries() {
        return Map.ofEntries(
            Map.entry("match", text -> new MatchQueryBuilder(TEXT_FIELD_NAME, text)),
            Map.entry("match_phrase", text -> new MatchPhraseQueryBuilder(TEXT_FIELD_NAME, text)),
            Map.entry("match_phrase_prefix", text -> new MatchPhrasePrefixQueryBuilder(TEXT_FIELD_NAME, text)),
            Map.entry("match_bool_prefix", text -> new MatchBoolPrefixQueryBuilder(TEXT_FIELD_NAME, text)),
            Map.entry("multi_match", text -> new MultiMatchQueryBuilder(text, TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME)),
            Map.entry("query_string", text -> new QueryStringQueryBuilder(text).defaultField(TEXT_FIELD_NAME)),
            Map.entry("simple_query_string", text -> new SimpleQueryStringBuilder(text).field(TEXT_FIELD_NAME)),
            Map.entry("combined_fields", text -> new CombinedFieldsQueryBuilder(text, TEXT_FIELD_NAME)),
            Map.entry(
                "intervals",
                text -> new IntervalQueryBuilder(TEXT_FIELD_NAME, new IntervalsSourceProvider.Match(text, -1, false, null, null, null))
            )
        );
    }

    public void testLongTextTripsBreakerWhileTokensAreCached() throws IOException {
        String text = "ab,".repeat(50_000);
        for (Map.Entry<String, Function<String, QueryBuilder>> entry : queries().entrySet()) {
            CircuitBreaker breaker = createCircuitBreakerService("1mb");
            SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), breaker);
            try {
                QueryBuilder query = entry.getValue().apply(text);
                CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> query.toQuery(context));
                assertThat(entry.getKey(), e.getMessage(), containsString("data for [query_tokens:" + TEXT_FIELD_NAME + "]"));
                assertThat(entry.getKey() + " must release the token charge on the trip", breaker.getUsed(), equalTo(0L));
            } finally {
                context.releaseQueryConstructionMemory();
            }
        }
    }

    public void testTokenChargeIsReleasedOnceQueryIsBuilt() throws IOException {
        // Long enough to charge the breaker at least once (about 350 bytes for a 52-character token), with few enough tokens to stay
        // under the default clause limit of 1024 when multi_match builds one clause per token for each of its two fields.
        String text = ("abcdefghijklmnopqrstuvwxyz".repeat(2) + ",").repeat(500);
        for (Map.Entry<String, Function<String, QueryBuilder>> entry : queries().entrySet()) {
            TokenChargeRecorder breaker = new TokenChargeRecorder();
            SearchExecutionContext context = new SearchExecutionContext(createSearchExecutionContext(), breaker);
            try {
                entry.getValue().apply(text).toQuery(context);
                assertThat(entry.getKey() + " must charge the cached tokens", breaker.tokenBytes.get(), greaterThan(0L));
                assertThat(
                    entry.getKey() + " must hold only the built query's charge",
                    breaker.getUsed(),
                    equalTo(context.getQueryConstructionMemoryUsed())
                );
            } finally {
                context.releaseQueryConstructionMemory();
            }
            assertThat(entry.getKey(), breaker.getUsed(), equalTo(0L));
        }
    }

    /**
     * Sums what is charged under {@link ChildMemoryCircuitBreaker#CATEGORY_QUERY_TOKENS}. The token charge is released before the built
     * query's charge is committed, so neither the final nor the peak usage of a breaker shows whether the tokens were charged at all.
     */
    private static final class TokenChargeRecorder extends TrackingCircuitBreaker {
        private final AtomicLong tokenBytes = new AtomicLong();

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            super.addEstimateBytesAndMaybeBreak(bytes, label);
            if (label.startsWith(ChildMemoryCircuitBreaker.CATEGORY_QUERY_TOKENS + ":")) {
                tokenBytes.addAndGet(bytes);
            }
        }
    }
}
