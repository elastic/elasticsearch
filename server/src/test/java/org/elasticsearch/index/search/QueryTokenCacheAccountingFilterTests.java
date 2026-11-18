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
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
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
     * The estimate is compared with the measured size of the states that {@link CachingTokenFilter} captures, for each set of term
     * attributes a token stream can have.
     */
    public void testEstimateIsCloseToMeasuredSize() throws IOException {
        TermAttributes termAttributes = randomFrom(TermAttributes.values());
        int numTokens = between(1000, 5000);
        try (Analyzer analyzer = new StandardAnalyzer()) {
            List<AttributeSource.State> states;
            long estimated;
            try (
                TokenStream source = tokenStream(analyzer, termAttributes, numTokens);
                QueryTokenCacheAccountingFilter filter = new QueryTokenCacheAccountingFilter(source, null, FIELD)
            ) {
                states = captureAll(filter);
                estimated = filter.estimatedBytes();
            }
            assertThat(states.size(), equalTo(numTokens));
            // An array, because RamUsageTester leaves out a JDK list's slots, which the estimate counts.
            long measured = RamUsageTester.ramUsed(states.toArray(new AttributeSource.State[0]));
            assertThat(
                termAttributes + ": estimate " + estimated + " vs measured " + measured,
                (double) estimated,
                closeTo(measured, measured * 0.1)
            );
        }
    }

    /**
     * Query parsers skip a stream without a term attribute, so wrapping one must not add it.
     */
    public void testDoesNotAddTermAttributes() throws IOException {
        try (
            TokenStream source = new GeneratedTokenStream(between(1, 10), false);
            QueryTokenCacheAccountingFilter filter = new QueryTokenCacheAccountingFilter(source, null, FIELD)
        ) {
            assertFalse(filter.hasAttribute(TermToBytesRefAttribute.class));
            assertFalse(filter.hasAttribute(CharTermAttribute.class));
        }
    }

    public void testShortTextDoesNotTouchBreaker() throws IOException {
        LimitedBreaker breaker = new LimitedBreaker("test", ByteSizeValue.ofMb(100));
        try (
            Analyzer analyzer = new StandardAnalyzer();
            TokenStream source = analyzer.tokenStream(FIELD, repeatedTokens(between(1, 100)))
        ) {
            QueryTokenCacheAccountingFilter filter = new QueryTokenCacheAccountingFilter(source, breaker, FIELD);
            try (CachingTokenFilter stream = new CachingTokenFilter(filter)) {
                consume(stream);
                assertThat(filter.estimatedBytes(), greaterThan(0L));
                assertThat(filter.estimatedBytes(), lessThan(QueryTokenCacheAccountingFilter.CHARGE_BATCH_BYTES));
                assertThat(breaker.getUsed(), equalTo(0L));
            }
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

    /** The term attributes a token stream can have, which decide what each cached token holds. */
    private enum TermAttributes {
        /** Both term attributes, from the {@code CharTermAttributeImpl} that analyzers use. */
        CHARS_AND_BYTES,
        /** Only {@link TermToBytesRefAttribute}, from a {@code BytesTermAttributeImpl}. */
        BYTES_ONLY,
        /** No term attribute. */
        NONE
    }

    private static TokenStream tokenStream(Analyzer analyzer, TermAttributes termAttributes, int numTokens) throws IOException {
        return switch (termAttributes) {
            case CHARS_AND_BYTES -> {
                if (randomBoolean()) {
                    leaveLongTermInBuilder(analyzer, between(1, 255));
                }
                StringBuilder text = new StringBuilder();
                for (int i = 0; i < numTokens; i++) {
                    text.append(randomAlphaOfLengthBetween(1, 20)).append(' ');
                }
                yield analyzer.tokenStream(FIELD, text.toString());
            }
            case BYTES_ONLY -> new GeneratedTokenStream(numTokens, true);
            case NONE -> new GeneratedTokenStream(numTokens, false);
        };
    }

    /**
     * Emits tokens of random bytes, or tokens with no term at all. Lucene's own canned binary stream also gets a
     * {@link CharTermAttribute} from its attribute factory, so it cannot stand in for a stream that only has bytes.
     */
    private static final class GeneratedTokenStream extends TokenStream {
        private final PositionIncrementAttribute posIncAtt;
        @Nullable
        private final BytesTermAttribute bytesAtt;
        private final int numTokens;
        private int emitted;

        GeneratedTokenStream(int numTokens, boolean withBytes) {
            // The default factory gives each attribute its own implementation, unlike the token factory.
            super(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY);
            this.numTokens = numTokens;
            this.posIncAtt = addAttribute(PositionIncrementAttribute.class);
            this.bytesAtt = withBytes ? addAttribute(BytesTermAttribute.class) : null;
        }

        @Override
        public boolean incrementToken() {
            if (emitted == numTokens) {
                return false;
            }
            clearAttributes();
            posIncAtt.setPositionIncrement(1);
            if (bytesAtt != null) {
                bytesAtt.setBytesRef(new BytesRef(randomByteArrayOfLength(between(1, 20))));
            }
            emitted++;
            return true;
        }

        @Override
        public void reset() throws IOException {
            super.reset();
            emitted = 0;
        }
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
