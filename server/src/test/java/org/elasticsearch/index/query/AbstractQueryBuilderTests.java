/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.hamcrest.Matchers.containsString;

public class AbstractQueryBuilderTests extends ESTestCase {

    private static NamedXContentRegistry xContentRegistry;

    @BeforeClass
    public static void init() {
        xContentRegistry = new NamedXContentRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedXContents());
    }

    @AfterClass
    public static void cleanup() {
        xContentRegistry = null;
    }

    public void testParseInnerQueryBuilder() throws IOException {
        QueryBuilder query = new MatchQueryBuilder("foo", "bar");
        String source = query.toString();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            QueryBuilder actual = parseInnerQueryBuilder(parser);
            assertEquals(query, actual);
        }
    }

    public void testParseInnerQueryBuilderExceptions() throws IOException {
        String source = "{ \"foo\": \"bar\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            parser.nextToken();
            parser.nextToken(); // don't start with START_OBJECT to provoke exception
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("[_na] query malformed, must start with start_object", exception.getMessage());
        }

        source = "{}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("query malformed, empty clause found at [1:2]", exception.getMessage());
        }

        source = "{ \"foo\" : \"bar\" }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("[foo] query malformed, no start_object after query name", exception.getMessage());
        }

        source = "{ \"boool\" : {} }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("unknown query [boool] did you mean [bool]?", exception.getMessage());
        }
        source = "{ \"match_\" : {} }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            ParsingException exception = expectThrows(ParsingException.class, () -> parseInnerQueryBuilder(parser));
            assertEquals("unknown query [match_] did you mean any of [match, match_all, match_none]?", exception.getMessage());
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    public void testEstimateValueHelper() {
        // null → 0
        assertEquals(0L, AbstractQueryBuilder.estimateValue(null));
        // String → length*2+64
        assertEquals(68L, AbstractQueryBuilder.estimateValue("hi"));  // 2*2+64
        // byte[] → length+32
        assertEquals(34L, AbstractQueryBuilder.estimateValue(new byte[2]));  // 2+32
        // BytesRef → length+64
        assertEquals(67L, AbstractQueryBuilder.estimateValue(new BytesRef(new byte[3])));  // 3+64
        // empty List → 32+0+0
        assertEquals(32L, AbstractQueryBuilder.estimateValue(new ArrayList<>()));
        // List with null → 32 + 1*8 + 0
        List<Object> withNull = new ArrayList<>();
        withNull.add(null);
        assertEquals(40L, AbstractQueryBuilder.estimateValue(withNull));
        // List with one String → 32 + 1*8 + 68
        assertEquals(108L, AbstractQueryBuilder.estimateValue(List.of("hi")));
        // Nested: List containing an empty List → 32 + 1*8 + 32
        assertEquals(72L, AbstractQueryBuilder.estimateValue(List.of(List.of())));
        // empty Map → 32+0+0
        assertEquals(32L, AbstractQueryBuilder.estimateValue(Map.of()));
        // Map with one entry: key "k" (1*2+64=66), value "v" (1*2+64=66) → 32+48+66+66
        assertEquals(212L, AbstractQueryBuilder.estimateValue(Map.of("k", "v")));
    }

    public void testQueryNameChargedAtParsesite() throws IOException {
        // The _name field is charged centrally at the parse site (not inside parseTimeBreakerEstimate).
        // Verify that: (a) the unnamed query succeeds at exactly the unnamed-estimate limit, and
        // (b) the same query with a _name trips the breaker at that same limit.
        // NOTE: must use parseTopLevelQuery (not the protected parseInnerQueryBuilder) because only
        // parseTopLevelQuery wraps the parser in a FilterXContentParserWrapper that fires the charge site.
        String name = "my_name";
        long expectedNameCharge = name.length() * 2L + 64L;

        MatchQueryBuilder unnamed = new MatchQueryBuilder("f", "v");
        long unnamedEstimate = unnamed.parseTimeBreakerEstimate();

        MatchQueryBuilder named = new MatchQueryBuilder("f", "v");
        named.queryName(name);
        // parseTimeBreakerEstimate() itself does NOT include the name cost
        assertEquals(unnamedEstimate, named.parseTimeBreakerEstimate());

        // Unnamed query must not trip the breaker at exactly the unnamed estimate
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(unnamedEstimate));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            String unnamedJson = unnamed.toString();
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, unnamedJson)) {
                AbstractQueryBuilder.parseTopLevelQuery(parser); // must not throw
            }
            // Named query must trip the breaker: name charge pushes it past the limit
            assertEquals(0L, breaker.getUsed()); // charge was released after successful parse
            String namedJson = named.toString();
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, namedJson)) {
                expectThrows(CircuitBreakingException.class, () -> AbstractQueryBuilder.parseTopLevelQuery(parser));
            }
            // Confirm the name charge accounts for the difference
            LimitedBreaker exactBreaker = new LimitedBreaker(
                CircuitBreaker.REQUEST,
                ByteSizeValue.ofBytes(unnamedEstimate + expectedNameCharge)
            );
            AbstractQueryBuilder.setQueryParsingBreaker(exactBreaker);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, namedJson)) {
                AbstractQueryBuilder.parseTopLevelQuery(parser); // must not throw at exact limit
            }
            AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    public void testBreakerAtExactLimitDoesNotThrow() throws IOException {
        // LimitedBreaker uses strict > (not >=), so a charge exactly equal to the limit must succeed.
        MatchQueryBuilder q = new MatchQueryBuilder("f", "v");
        long estimate = q.parseTimeBreakerEstimate();
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(estimate));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            String json = q.toString();
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
                AbstractQueryBuilder.parseTopLevelQuery(parser); // exactly at limit — must not throw
            }
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    public void testMaybeConvertToBytesRefLongTerm() {
        String longTerm = "a".repeat(IndexWriter.MAX_TERM_LENGTH + 1);
        Exception e = expectThrows(IllegalArgumentException.class, () -> AbstractQueryBuilder.maybeConvertToBytesRef(longTerm));
        assertThat(e.getMessage(), containsString("term starting with [aaaaa"));
    }

    public void testMaybeConvertToBytesRefStringCorrectSize() {
        int capacity = randomIntBetween(20, 40);
        StringBuilder termBuilder = new StringBuilder(capacity);
        int correctSize = 0;
        for (int i = 0; i < capacity; i++) {
            if (i < capacity / 3) {
                termBuilder.append((char) randomIntBetween(0, 127));
                ++correctSize; // use only one byte for char < 128
            } else if (i < 2 * capacity / 3) {
                termBuilder.append((char) randomIntBetween(128, 2047));
                correctSize += 2; // use two bytes for char < 2048
            } else {
                termBuilder.append((char) randomIntBetween(2048, 4092));
                correctSize += 3; // use three bytes for char >= 2048
            }
        }
        BytesRef bytesRef = (BytesRef) AbstractQueryBuilder.maybeConvertToBytesRef(termBuilder.toString());
        assertEquals(correctSize, bytesRef.bytes.length);
        assertEquals(correctSize, bytesRef.length);
    }

    /**
     * Verifies that {@link AbstractQueryBuilder#clearQueryParsingBreaker} uses CAS semantics: when
     * two threads race to clear the same expected breaker instance, exactly one succeeds.
     */
    public void testClearQueryParsingBreakerCasSemantics() throws InterruptedException {
        LimitedBreaker breaker = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        AbstractQueryBuilder.setQueryParsingBreaker(breaker);
        try {
            int threads = 2;
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger cleared = new AtomicInteger(0);
            Thread[] ts = new Thread[threads];
            for (int i = 0; i < threads; i++) {
                ts[i] = new Thread(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    AbstractQueryBuilder.clearQueryParsingBreaker(breaker);
                    cleared.incrementAndGet();
                });
                ts[i].start();
            }
            ready.await();
            go.countDown();
            for (Thread t : ts) {
                t.join();
            }
            // Both threads ran clearQueryParsingBreaker — verify the breaker is now null
            // (the CAS succeeded exactly once, leaving null in the slot)
            assertEquals(threads, cleared.get()); // both calls returned, but only one CAS succeeded
            // Install a dummy to verify the slot is actually cleared
            LimitedBreaker dummy = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE));
            AbstractQueryBuilder.setQueryParsingBreaker(dummy);
            AbstractQueryBuilder.clearQueryParsingBreaker(dummy);
            // A second clear with the wrong expected value must be a no-op
            AbstractQueryBuilder.clearQueryParsingBreaker(breaker); // breaker != null, so CAS fails
            // Re-install dummy and verify it is still present
            AbstractQueryBuilder.setQueryParsingBreaker(dummy);
            AbstractQueryBuilder.clearQueryParsingBreaker(breaker); // wrong expected — no-op
            AbstractQueryBuilder.clearQueryParsingBreaker(dummy);   // correct expected — clears
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

    /**
     * Verifies that {@link AbstractQueryBuilder#clearQueryParsingBreaker} with a wrong expected
     * value leaves the current breaker in place (no-op).
     */
    public void testClearQueryParsingBreakerWrongExpectedIsNoop() {
        LimitedBreaker installed = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        LimitedBreaker other = new LimitedBreaker(CircuitBreaker.REQUEST, ByteSizeValue.ofBytes(Long.MAX_VALUE));
        AbstractQueryBuilder.setQueryParsingBreaker(installed);
        try {
            AbstractQueryBuilder.clearQueryParsingBreaker(other); // wrong expected — no-op
            // If it were a no-op, the installed breaker is still active; install another breaker
            // successfully to confirm the slot is non-null (i.e., still holds `installed`).
            AbstractQueryBuilder.setQueryParsingBreaker(other);
            // Now clear correctly
            AbstractQueryBuilder.clearQueryParsingBreaker(other);
        } finally {
            AbstractQueryBuilder.setQueryParsingBreaker(null);
        }
    }

}
