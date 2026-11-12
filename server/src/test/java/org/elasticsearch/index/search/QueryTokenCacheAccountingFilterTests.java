/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.AttributeSource;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class QueryTokenCacheAccountingFilterTests extends ESTestCase {

    private static final String FIELD = "field";

    /**
     * A reused token stream whose term attribute last encoded a long term must not copy that term into every cached token.
     */
    public void testCachedTokensDoNotCarryStaleTermBytes() throws IOException {
        try (StandardAnalyzer analyzer = new StandardAnalyzer()) {
            analyzer.setMaxTokenLength(4096);
            leaveLongTermInBuilder(analyzer, 4000);

            int numTokens = between(100, 1000);
            List<AttributeSource.State> states;
            try (
                TokenStream source = analyzer.tokenStream(FIELD, repeatedTokens(numTokens));
                QueryTokenCacheAccountingFilter filter = new QueryTokenCacheAccountingFilter(source, null, FIELD)
            ) {
                states = captureAll(filter);
            }
            assertThat(states.size(), equalTo(numTokens));
            long bytesPerToken = RamUsageTester.ramUsed(states) / numTokens;
            assertThat("cached tokens must not hold a copy of the stale 4000 byte term", bytesPerToken, lessThan(1000L));
        }
    }

    /**
     * The estimate is compared with the measured size of the states that {@link CachingTokenFilter} captures.
     */
    public void testEstimateIsCloseToMeasuredSize() throws IOException {
        try (Analyzer analyzer = new StandardAnalyzer()) {
            if (randomBoolean()) {
                leaveLongTermInBuilder(analyzer, between(1, 255));
            }
            int numTokens = between(1000, 5000);
            StringBuilder text = new StringBuilder();
            for (int i = 0; i < numTokens; i++) {
                text.append(randomAlphaOfLengthBetween(1, 20)).append(' ');
            }
            List<AttributeSource.State> states;
            long estimated;
            try (
                TokenStream source = analyzer.tokenStream(FIELD, text.toString());
                QueryTokenCacheAccountingFilter filter = new QueryTokenCacheAccountingFilter(source, null, FIELD)
            ) {
                states = captureAll(filter);
                estimated = filter.estimatedBytes();
            }
            long measured = RamUsageTester.ramUsed(states);
            assertThat("estimate " + estimated + " vs measured " + measured, (double) estimated, greaterThan(measured * 0.75));
            assertThat("estimate " + estimated + " vs measured " + measured, (double) estimated, lessThan(measured * 1.25));
        }
    }

    public void testShortTextDoesNotTouchBreaker() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(100));
        try (
            Analyzer analyzer = new StandardAnalyzer();
            TokenStream source = analyzer.tokenStream(FIELD, repeatedTokens(between(1, 100)));
            CachingTokenFilter stream = new CachingTokenFilter(new QueryTokenCacheAccountingFilter(source, breaker, FIELD))
        ) {
            consume(stream);
            assertThat(breaker.getUsed(), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testLongTextIsChargedWhileCachedAndReleasedOnClose() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(100));
        try (Analyzer analyzer = new StandardAnalyzer(); TokenStream source = analyzer.tokenStream(FIELD, repeatedTokens(20_000))) {
            QueryTokenCacheAccountingFilter filter = new QueryTokenCacheAccountingFilter(source, breaker, FIELD);
            try (CachingTokenFilter stream = new CachingTokenFilter(filter)) {
                consume(stream);
                long estimated = filter.estimatedBytes();
                assertThat(breaker.getUsed(), greaterThan(0L));
                assertThat(breaker.getUsed(), lessThanOrEqualTo(estimated));
                assertThat(estimated - breaker.getUsed(), lessThan(QueryTokenCacheAccountingFilter.CHARGE_BATCH_BYTES));
                // Replaying the cache reads no tokens from the filter, so it charges nothing more.
                long charged = breaker.getUsed();
                consume(stream);
                assertThat(breaker.getUsed(), equalTo(charged));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testTripReleasesWhatWasCharged() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofKb(between(100, 500)));
        try (
            Analyzer analyzer = new StandardAnalyzer();
            TokenStream source = analyzer.tokenStream(FIELD, repeatedTokens(50_000));
            CachingTokenFilter stream = new CachingTokenFilter(new QueryTokenCacheAccountingFilter(source, breaker, FIELD))
        ) {
            stream.reset();
            expectThrows(CircuitBreakingException.class, stream::incrementToken);
            assertThat(breaker.getTrippedCount(), equalTo(1L));
            assertThat(breaker.getUsed(), greaterThan(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    /**
     * Analyzes a single long term and encodes it, as a highlighter or an earlier query on this thread would, so the analyzer's
     * reused term attribute keeps those bytes.
     */
    private static void leaveLongTermInBuilder(Analyzer analyzer, int length) throws IOException {
        try (TokenStream ts = analyzer.tokenStream(FIELD, "a".repeat(length))) {
            TermToBytesRefAttribute termAtt = ts.getAttribute(TermToBytesRefAttribute.class);
            ts.reset();
            assertTrue(ts.incrementToken());
            assertThat(ts.getAttribute(CharTermAttribute.class).length(), equalTo(length));
            assertThat(termAtt.getBytesRef().length, equalTo(length));
            ts.end();
        }
    }

    private static String repeatedTokens(int numTokens) {
        return "ab ".repeat(numTokens);
    }

    /**
     * Captures one state per token, as {@link CachingTokenFilter} does.
     */
    private static List<AttributeSource.State> captureAll(TokenStream ts) throws IOException {
        List<AttributeSource.State> states = new ArrayList<>();
        ts.reset();
        while (ts.incrementToken()) {
            states.add(ts.captureState());
        }
        ts.end();
        return states;
    }

    private static void consume(TokenStream ts) throws IOException {
        ts.reset();
        while (ts.incrementToken()) {
            // read every token
        }
        ts.end();
    }
}
