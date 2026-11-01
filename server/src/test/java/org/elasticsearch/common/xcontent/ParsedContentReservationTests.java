/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests that a tracked parser charges for every token however it is read, that the breaker is checked each time a request charges a
 * chunk, that the size a tracked object reports replaces the estimate for its tokens, and that the charge is held until the reservation
 * closes.
 */
public class ParsedContentReservationTests extends ESTestCase {

    private static final XContentType[] TYPES = { XContentType.JSON, XContentType.SMILE, XContentType.CBOR, XContentType.YAML };

    public void testChargesEveryTokenAndStringLength() throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofMb(100));
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, between(2, 100));
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            try (XContentParser parser = reservation.track(parser(type, content))) {
                readAllTokens(parser);
            }
            long expected = expectedCharge(type, content);
            assertThat(reservation.reservedBytes(), equalTo(expected));
            assertThat(breaker.getUsed(), equalTo(expected));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSmileAndCborAreChargedLikeJson() throws IOException {
        int clauses = between(2, 100);
        long jsonCharge = charge(XContentType.JSON, document(XContentType.JSON, clauses), ParsedContentReservationTests::readAllTokens);
        for (XContentType type : List.of(XContentType.SMILE, XContentType.CBOR)) {
            assertThat(
                type.toString(),
                charge(type, document(type, clauses), ParsedContentReservationTests::readAllTokens),
                equalTo(jsonCharge)
            );
        }
        // YAML makes a new string for each occurrence of a field name, so each one is charged.
        assertThat(
            charge(XContentType.YAML, document(XContentType.YAML, clauses), ParsedContentReservationTests::readAllTokens),
            greaterThan(jsonCharge)
        );
    }

    public void testMapAndListReadsAreChargedLikeTokens() throws IOException {
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, between(1, 100));
        long tokenByToken = charge(type, content, ParsedContentReservationTests::readAllTokens);
        assertThat(tokenByToken, greaterThan(0L));
        // FilterXContentParser hands the bulk reads to the wrapped parser, which would read past the tracked one.
        assertThat(charge(type, content, XContentParser::map), equalTo(tokenByToken));
        assertThat(charge(type, content, XContentParser::mapOrdered), equalTo(tokenByToken));
        assertThat(
            charge(type, content, p -> p.map(HashMap::new, v -> AbstractXContentParser.readValue(v, HashMap::new))),
            equalTo(tokenByToken)
        );

        BytesReference strings = stringMap(type, between(1, 100));
        assertThat(
            charge(type, strings, XContentParser::mapStrings),
            equalTo(charge(type, strings, ParsedContentReservationTests::readAllTokens))
        );

        BytesReference array = array(type, between(2, 100));
        long arrayTokens = charge(type, array, ParsedContentReservationTests::readAllTokens);
        assertThat(charge(type, array, XContentParser::list), equalTo(arrayTokens));
        assertThat(charge(type, array, XContentParser::listOrderedMap), equalTo(arrayTokens));
    }

    public void testNamedObjectIsReadThroughTrackedParser() throws IOException {
        XContentType type = randomFrom(TYPES);
        String name = "thing";
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject(name);
            int entries = between(1, 50);
            for (int i = 0; i < entries; i++) {
                builder.field("key-" + i, "value-" + i);
            }
            builder.endObject().endObject();
            content = BytesReference.bytes(builder);
        }
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofMb(100));
        XContentParser[] given = new XContentParser[1];
        NamedXContentRegistry registry = new NamedXContentRegistry(
            List.of(new NamedXContentRegistry.Entry(Object.class, new ParseField(name), p -> {
                given[0] = p;
                return p.map();
            }))
        );
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            try (XContentParser tracked = reservation.track(parser(registry, type, content))) {
                assertThat(tracked.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(tracked.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                tracked.nextToken();
                tracked.namedObject(Object.class, name, null);
                // FilterXContentParser would hand over the wrapped parser instead.
                assertSame(tracked, given[0]);
                assertThat(tracked.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                assertNull(tracked.nextToken());
            }
            assertThat(reservation.reservedBytes(), equalTo(expectedCharge(type, content)));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testTripsWhileReading() throws IOException {
        ByteSizeValue limit = ByteSizeValue.ofBytes(2 * ChargingXContentParser.CHUNK_BYTES);
        CircuitBreaker breaker = requestBreaker(limit);
        XContentType type = randomFrom(TYPES);
        // A few hundred bytes a clause, so several times the limit.
        BytesReference content = document(type, 20_000);
        long totalTokens = countTokens(type, content);

        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            long tokensRead = 0;
            CircuitBreakingException e = null;
            try (XContentParser parser = reservation.track(parser(type, content))) {
                while (parser.nextToken() != null) {
                    tokensRead++;
                }
            } catch (CircuitBreakingException ex) {
                e = ex;
            }
            assertNotNull("expected the breaker to trip", e);
            assertThat(e.getMessage(), containsString("data for [" + ParsedContentReservation.LABEL + "]"));
            assertThat(tokensRead, lessThan(totalTokens / 2));
            assertThat(reservation.reservedBytes(), lessThanOrEqualTo(limit.getBytes()));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSmallContentIsChargedWhenItsRootEnds() throws IOException {
        // Any check would trip.
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(1));
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, between(1, 10));
        long expected = expectedCharge(type, content);
        long tokens = countTokens(type, content);
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            XContentParser parser = reservation.track(parser(type, content));
            for (long i = 1; i < tokens; i++) {
                parser.nextToken();
            }
            assertThat(reservation.reservedBytes(), equalTo(0L));
            // Charged without a check when the root ends, not at close, since handlers do not always close their parsers.
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
            assertThat(reservation.reservedBytes(), equalTo(expected));
            assertThat("the next check takes it in", reservation.uncheckedBytes(), equalTo(expected));
            assertThat(breaker.getTrippedCount(), equalTo(0L));
            parser.close();
            assertThat(reservation.reservedBytes(), equalTo(expected));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSkippedChildrenAreNotCharged() throws IOException {
        XContentType type = randomFrom(TYPES);
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().field("skipped");
            try (XContentParser skipped = parser(type, document(type, between(1, 100)))) {
                builder.copyCurrentStructure(skipped);
            }
            builder.field("kept", 1).endObject();
            content = BytesReference.bytes(builder);
        }
        BytesReference withoutSkipped;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject("skipped").endObject().field("kept", 1).endObject();
            withoutSkipped = BytesReference.bytes(builder);
        }
        try (ParsedContentReservation reservation = new ParsedContentReservation(requestBreaker(ByteSizeValue.ofMb(100)), true)) {
            XContentParser parser = reservation.track(parser(type, content));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            assertThat(parser.nextFieldName(), equalTo("skipped"));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
            parser.skipChildren();
            assertThat(parser.nextFieldName(), equalTo("kept"));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
            // Everything read but the skipped object's end, charged when the root ended.
            long expected = expectedCharge(type, withoutSkipped) - ChargingXContentParser.TOKEN_BYTES;
            assertThat(reservation.reservedBytes(), equalTo(expected));
            parser.close();
            assertThat(reservation.reservedBytes(), equalTo(expected));
        }
    }

    public void testParserIsTrackedBeforeItReads() throws IOException {
        XContentType type = randomFrom(TYPES);
        try (ParsedContentReservation reservation = new ParsedContentReservation(requestBreaker(ByteSizeValue.ofMb(100)), true)) {
            try (XContentParser started = parser(type, document(type, 1))) {
                started.nextToken();
                // A parser that started reading cannot tell where its root ends.
                expectThrows(AssertionError.class, () -> reservation.track(started));
            }
        }
    }

    public void testTrackedRootIsChargedItsSizeWithoutClose() throws IOException {
        XContentType type = randomFrom(TYPES);
        BytesReference content = stringMap(type, between(1, 50));
        // The estimate for 1 to 50 entries is 308 to about 11,000 bytes, so one size releases part of it and the other adds to it.
        for (long size : new long[] { 100, 20_000 }) {
            try (ParsedContentReservation reservation = new ParsedContentReservation(requestBreaker(ByteSizeValue.ofMb(100)), true)) {
                XContentParser parser = reservation.track(parser(type, content));
                // The root itself is tracked, so it ends before its size replaces the estimate.
                ParsedObjectTracker.of(parser).parseSized(parser::map, map -> size);
                assertThat(reservation.reservedBytes(), equalTo(size + distinctNames(type, content)));
                parser.close();
                assertThat(reservation.reservedBytes(), equalTo(size + distinctNames(type, content)));
            }
        }
    }

    public void testParserWithItsOwnReservationIsCheckedAndReleased() throws IOException {
        XContentType type = randomFrom(TYPES);
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofMb(100));
        try (XContentParser parser = ParsedContentReservation.trackWhileParsing(breaker, parser(type, document(type, 20_000)))) {
            readAllTokens(parser);
            assertThat(breaker.getUsed(), greaterThan(0L));
        }
        assertThat("released when the parser is closed", breaker.getUsed(), equalTo(0L));

        CircuitBreaker tight = requestBreaker(ByteSizeValue.ofBytes(2 * ChargingXContentParser.CHUNK_BYTES));
        try (XContentParser parser = ParsedContentReservation.trackWhileParsing(tight, parser(type, document(type, 20_000)))) {
            expectThrows(CircuitBreakingException.class, () -> readAllTokens(parser));
        }
        assertThat(tight.getUsed(), equalTo(0L));
    }

    public void testSmallParsersOfOneRequestAreCheckedTogether() throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(1));
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, between(1, 10));
        long perParser = expectedCharge(type, content);
        // As many small parsers, such as the lines of an _msearch, as charge less than a chunk together, so none is checked.
        long underAChunk = (ChargingXContentParser.CHUNK_BYTES - 1) / perParser;
        assertThat(underAChunk, greaterThan(1L));
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            for (long i = 0; i < underAChunk; i++) {
                try (XContentParser parser = reservation.track(parser(type, content))) {
                    readAllTokens(parser);
                }
            }
            assertThat(reservation.reservedBytes(), equalTo(underAChunk * perParser));
            assertThat(breaker.getTrippedCount(), equalTo(0L));

            // The parser with which the request reaches a chunk is checked while it is read.
            try (XContentParser parser = reservation.track(parser(type, content))) {
                CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> readAllTokens(parser));
                assertThat(e.getMessage(), containsString("data for [" + ParsedContentReservation.LABEL + "]"));
            }
            assertThat(reservation.reservedBytes(), equalTo(underAChunk * perParser));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testCheckTakesInWhatWasChargedWithoutOne() throws IOException {
        // The check at the first chunk fits under the limit, and the one at the second does not.
        ByteSizeValue limit = ByteSizeValue.ofBytes(ChargingXContentParser.CHUNK_BYTES * 3 / 2);
        CircuitBreaker breaker = requestBreaker(limit);
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, between(1, 10));
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            expectThrows(CircuitBreakingException.class, () -> {
                while (true) {
                    try (XContentParser parser = reservation.track(parser(type, content))) {
                        readAllTokens(parser);
                    }
                    assertThat(reservation.uncheckedBytes(), lessThan(ChargingXContentParser.CHUNK_BYTES));
                }
            });
            assertThat(breaker.getTrippedCount(), equalTo(1L));
            // Over the limit, since the second check came a chunk after the first rather than at the next token.
            assertThat("not checked again until another chunk", reservation.reservedBytes(), greaterThan(limit.getBytes()));
            assertThat(reservation.reservedBytes(), lessThanOrEqualTo(limit.getBytes() + ChargingXContentParser.CHUNK_BYTES));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testParsersOpenAtTheSameTime() throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(1));
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, between(1, 10));
        long perParser = expectedCharge(type, content);
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            XContentParser first = reservation.track(parser(type, content));
            XContentParser second = reservation.track(parser(type, content));
            // Read in turn, each charges what it reads.
            XContentParser.Token token;
            do {
                token = first.nextToken();
                assertThat(second.nextToken(), equalTo(token));
            } while (token != null);
            assertThat(reservation.reservedBytes(), equalTo(2 * perParser));
            assertThat(breaker.getTrippedCount(), equalTo(0L));

            // A parser that charges a chunk is checked while the others are still open.
            try (XContentParser large = reservation.track(parser(type, document(type, 20_000)))) {
                expectThrows(CircuitBreakingException.class, () -> readAllTokens(large));
            }
            first.close();
            second.close();
            assertThat(reservation.reservedBytes(), equalTo(2 * perParser));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testChecksAtTheSameTimeTakeInWhatWasUncheckedOnce() throws Exception {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofGb(10));
        AtomicBoolean belowZero = new AtomicBoolean();
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            runInParallel(between(2, 8), thread -> {
                for (int i = 0; i < 10_000; i++) {
                    reservation.reserveWithoutBreaking(7);
                    reservation.reserve(5);
                    // If two checks took in the same bytes, this would go below zero and put the next check more than a chunk away.
                    if (reservation.uncheckedBytes() < 0) {
                        belowZero.set(true);
                    }
                }
            });
            assertFalse("what was charged without a check went below zero", belowZero.get());
            assertThat(reservation.uncheckedBytes(), greaterThanOrEqualTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testReservationThatCannotTripChargesWithoutBreaking() throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(1));
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, 20_000);
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, false)) {
            try (XContentParser parser = reservation.track(parser(type, content))) {
                readAllTokens(parser);
            }
            assertThat(reservation.reservedBytes(), equalTo(expectedCharge(type, content)));
            assertThat(breaker.getUsed(), equalTo(reservation.reservedBytes()));
            assertThat(breaker.getTrippedCount(), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testParserReadingAfterTheRequestCompletedIsNotCharged() throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(1));
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, 20_000);
        ParsedContentReservation reservation = new ParsedContentReservation(breaker, true);
        XContentParser parser = reservation.track(parser(type, content));
        parser.nextToken();
        // The request completes, such as by failing, while a forked parse is still reading.
        reservation.close();
        readAllTokens(parser);
        parser.close();
        assertThat(reservation.reservedBytes(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
        // Not checked either, so it cannot trip the breaker that other requests share.
        assertThat(breaker.getTrippedCount(), equalTo(0L));
        // Closing again releases nothing more.
        reservation.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testTrackedObjectIsChargedItsReportedSize() throws IOException {
        XContentType type = randomFrom(TYPES);
        BytesReference content = namedObjectDocument(type, "sized", stringMap(type, between(1, 50)));
        long estimate = expectedCharge(type, content);
        long withoutItsOwnTokens = chargeOfNamedObject(type, content, 0, 0);
        // UNKNOWN, or any other negative size, keeps the estimate.
        assertThat(chargeOfNamedObject(type, content, SizedParsedObject.UNKNOWN, 0), equalTo(estimate));
        assertThat(chargeOfNamedObject(type, content, -randomLongBetween(2, 1000), 0), equalTo(estimate));
        assertThat(withoutItsOwnTokens, lessThan(estimate));
        long size = randomLongBetween(0, 100_000);
        assertThat(chargeOfNamedObject(type, content, size, 0), equalTo(withoutItsOwnTokens + size));
    }

    public void testObjectsInsideATrackedObjectKeepTheirCharges() throws IOException {
        XContentType type = randomFrom(TYPES);
        int inner = between(2, 20);
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject("outer");
            for (int i = 0; i < inner; i++) {
                builder.startObject("inner-" + i).field("key", "value-" + i).endObject();
            }
            builder.endObject().endObject();
            content = BytesReference.bytes(builder);
        }
        long estimate = expectedCharge(type, content);
        long outside = chargeOfNamedObject(type, content, 0, 0);
        long innerEstimates = chargeOfNamedObject(type, content, 0, SizedParsedObject.UNKNOWN) - outside;
        long outerSize = randomLongBetween(0, 10_000);
        long innerSize = randomLongBetween(0, 10_000);
        // The outer size replaces only what its own tokens were charged, whether or not the inner objects report a size.
        assertThat(chargeOfNamedObject(type, content, SizedParsedObject.UNKNOWN, SizedParsedObject.UNKNOWN), equalTo(estimate));
        assertThat(chargeOfNamedObject(type, content, outerSize, innerSize), equalTo(outside + outerSize + inner * innerSize));
        assertThat(chargeOfNamedObject(type, content, outerSize, SizedParsedObject.UNKNOWN), equalTo(outside + outerSize + innerEstimates));
        assertThat(
            chargeOfNamedObject(type, content, SizedParsedObject.UNKNOWN, innerSize),
            equalTo(estimate - innerEstimates + inner * innerSize)
        );
    }

    public void testLargeObjectIsCheckedWhileReadAndReleasedOnceParsed() throws IOException {
        XContentType type = randomFrom(TYPES);
        BytesReference content = namedObjectDocument(type, "sized", stringMap(type, 50_000));
        long estimate = expectedCharge(type, content);
        assertThat(estimate, greaterThan(4 * ChargingXContentParser.CHUNK_BYTES));
        long size = randomLongBetween(0, ChargingXContentParser.CHUNK_BYTES);

        // Under a limit above the estimate, the object is charged its size once parsed.
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(2 * estimate));
        long outside = chargeOfNamedObject(type, content, 0, 0);
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            try (XContentParser parser = reservation.track(namedObjectParser(type, content, size, 0))) {
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                String name = parser.nextFieldName();
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                parser.namedObject(Object.class, name, null);
                // Released as soon as the object is parsed, before the root ends.
                assertThat(breaker.getUsed(), lessThan(outside + size + ChargingXContentParser.CHUNK_BYTES));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                assertNull(parser.nextToken());
            }
            assertThat(reservation.reservedBytes(), equalTo(outside + size));
        }
        assertThat(breaker.getUsed(), equalTo(0L));

        // Under a limit below the estimate, reading it trips though its size would fit, since the estimate guards the heap until then.
        CircuitBreaker tight = requestBreaker(ByteSizeValue.ofBytes(estimate / 2));
        try (ParsedContentReservation reservation = new ParsedContentReservation(tight, true)) {
            try (XContentParser parser = reservation.track(namedObjectParser(type, content, size, 0))) {
                expectThrows(CircuitBreakingException.class, () -> readNamedObjectDocument(parser));
            }
        }
        assertThat(tight.getUsed(), equalTo(0L));
    }

    public void testReportedSizeAboveTheEstimateIsChecked() throws IOException {
        XContentType type = randomFrom(TYPES);
        BytesReference content = namedObjectDocument(type, "sized", stringMap(type, between(1, 50)));
        // Content this small is not checked while read, so the check comes when the size replaces the estimate.
        long size = 3 * ChargingXContentParser.CHUNK_BYTES;
        long outside = chargeOfNamedObject(type, content, 0, 0);
        assertThat(chargeOfNamedObject(type, content, size, 0), equalTo(outside + size));

        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(2 * ChargingXContentParser.CHUNK_BYTES));
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            try (XContentParser parser = reservation.track(namedObjectParser(type, content, size, 0))) {
                expectThrows(CircuitBreakingException.class, () -> readNamedObjectDocument(parser));
            }
            assertThat(breaker.getTrippedCount(), equalTo(1L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testFailureInsideATrackedObjectKeepsItsEstimate() throws IOException {
        XContentType type = randomFrom(TYPES);
        long outerSize = randomLongBetween(0, 10_000);
        long innerSize = randomLongBetween(0, 10_000);
        // Failing to parse or to size keeps the estimate, as UNKNOWN does, and the object after it is charged as usual.
        long unsized = chargeWithFirstInner(type, "unsized", outerSize, innerSize);
        assertThat(chargeWithFirstInner(type, "failing", outerSize, innerSize), equalTo(unsized));
        assertThat(chargeWithFirstInner(type, "badsize", outerSize, innerSize), equalTo(unsized));
    }

    public void testTripNestedInTrackedObjects() throws IOException {
        XContentType type = randomFrom(TYPES);
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject("o1").startObject("o2");
            // Charged just over a chunk, above the limit.
            builder.field("s", "x".repeat(350_000));
            builder.endObject().endObject().endObject();
            content = BytesReference.bytes(builder);
        }
        // Also when the parser of o1 catches the trip in o2 and goes on.
        for (boolean caught : new boolean[] { false, true }) {
            NamedXContentRegistry registry = new NamedXContentRegistry(
                List.of(new NamedXContentRegistry.Entry(Object.class, new ParseField("o1"), p -> {
                    assertThat(p.nextFieldName(), equalTo("o2"));
                    assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                    try {
                        p.namedObject(Object.class, "o2", null);
                    } catch (CircuitBreakingException e) {
                        if (caught == false) {
                            throw e;
                        }
                        // Go on after the object, as a lenient parser would. The tripped parser trips again.
                        assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                    }
                    assertThat(p.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                    return new Sized(10, null);
                }), new NamedXContentRegistry.Entry(Object.class, new ParseField("o2"), p -> new Sized(10, p.map())))
            );
            CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(1_000_000));
            try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
                try (XContentParser parser = reservation.track(parser(registry, type, content))) {
                    CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> readNamedObjectDocument(parser));
                    assertThat(e.getMessage(), containsString("data for [" + ParsedContentReservation.LABEL + "]"));
                }
                // The reservation and the breaker still agree.
                assertThat(reservation.reservedBytes(), greaterThanOrEqualTo(0L));
                assertThat(breaker.getUsed(), equalTo(reservation.reservedBytes()));
            }
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    public void testTripWrappedByAQueryParserIsTooManyRequests() throws IOException {
        XContentType type = randomFrom(XContentType.JSON, XContentType.SMILE, XContentType.CBOR);
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject("ids").startArray("values");
            for (int i = 0; i < 20_000; i++) {
                builder.value("id-" + i);
            }
            builder.endArray().endObject().endObject();
            content = BytesReference.bytes(builder);
        }
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(ChargingXContentParser.CHUNK_BYTES));
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            try (XContentParser parser = reservation.track(parser(searchRegistry(), type, content))) {
                // ObjectParser wraps the trip, and the ids parser wraps that again.
                ParsingException e = expectThrows(ParsingException.class, () -> AbstractQueryBuilder.parseTopLevelQuery(parser));
                assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.TOO_MANY_REQUESTS));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testTripInsideABoundingBoxIsTooManyRequests() throws IOException {
        XContentType type = randomFrom(XContentType.JSON, XContentType.SMILE, XContentType.CBOR);
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject("geo_bounding_box").startObject("location");
            // A GeoJSON corner is read to the end of its coordinates before they are checked.
            builder.startObject("top_left").field("type", "Point").startArray("coordinates");
            for (int i = 0; i < 40_000; i++) {
                builder.value(1.0);
            }
            builder.endArray().endObject();
            builder.startObject("bottom_right").field("type", "Point").array("coordinates", 2.0, 0.0).endObject();
            builder.endObject().endObject().endObject();
            content = BytesReference.bytes(builder);
        }
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofBytes(ChargingXContentParser.CHUNK_BYTES));
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            try (XContentParser parser = reservation.track(parser(searchRegistry(), type, content))) {
                // The bounding box parser reports what fails in it as a parse failure.
                ElasticsearchParseException e = expectThrows(
                    ElasticsearchParseException.class,
                    () -> AbstractQueryBuilder.parseTopLevelQuery(parser)
                );
                assertThat(ExceptionsHelper.status(e), equalTo(RestStatus.TOO_MANY_REQUESTS));
            }
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testQueryClausesAreChargedTheSizesTheyReport() throws IOException {
        XContentType type = randomFrom(XContentType.JSON, XContentType.SMILE, XContentType.CBOR);
        int clauses = between(2, 100);
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject("bool").startArray("should");
            for (int i = 0; i < clauses; i++) {
                builder.startObject().startObject("term").field("field", "value-" + i).endObject().endObject();
            }
            builder.endArray().endObject().endObject();
            content = BytesReference.bytes(builder);
        }
        try (ParsedContentReservation reservation = new ParsedContentReservation(requestBreaker(ByteSizeValue.ofMb(100)), true)) {
            BoolQueryBuilder bool;
            try (XContentParser parser = reservation.track(parser(searchRegistry(), type, content))) {
                // parseTopLevelQuery parses named objects through its own wrapper, which must still track them.
                bool = (BoolQueryBuilder) AbstractQueryBuilder.parseTopLevelQuery(parser);
            }
            // The tokens around the bool query (the root's start and end, its name and the start of its body), each name string once,
            // and the sizes the queries report.
            long expected = 3 * ChargingXContentParser.TOKEN_BYTES + ChargingXContentParser.BYTES_PER_CHAR * "bool".length()
                + ChargingXContentParser.TOKEN_BYTES + distinctNames(type, content) + bool.parsedRetainedBytes();
            for (QueryBuilder clause : bool.should()) {
                expected += ((TermQueryBuilder) clause).parsedRetainedBytes();
            }
            assertThat(reservation.reservedBytes(), equalTo(expected));
        }
    }

    public void testSearchSectionsAreChargedTheSizesTheyReport() throws IOException {
        XContentType type = randomFrom(XContentType.JSON, XContentType.SMILE, XContentType.CBOR);
        Map<String, Object> knn = new LinkedHashMap<>();
        knn.put("field", "vector");
        knn.put("query_vector", List.of(1.0f, 2.0f, 3.0f));
        knn.put("k", 5);
        knn.put("num_candidates", 10);
        Map<String, Object> runtimeMappings = new LinkedHashMap<>();
        runtimeMappings.put("day", Map.of("type", "keyword"));
        runtimeMappings.put("hour", Map.of("type", "long"));
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("text", "some text");
        params.put("number", 1);
        params.put("list", List.of("a", "b"));
        BytesReference content = document(
            type,
            Map.of(
                "knn",
                knn,
                "runtime_mappings",
                runtimeMappings,
                "script_fields",
                Map.of("field", Map.of("script", Map.of("source", "1", "params", params)))
            )
        );
        // Each section replaces the estimate for its tokens, besides its starting token and its names, with the size it reports.
        long expected = charge(type, content, ParsedContentReservationTests::readAllTokens);
        BytesReference knnContent = document(type, knn);
        long knnSize;
        try (XContentParser parser = parser(type, knnContent)) {
            knnSize = KnnSearchBuilder.fromXContent(parser).parsedRetainedBytes();
        }
        expected += replacedEstimate(type, knnContent, knnSize);
        for (Map<String, Object> map : List.of(runtimeMappings, params)) {
            BytesReference mapContent = document(type, map);
            long size;
            try (XContentParser parser = parser(type, mapContent)) {
                size = ParsedObjectSizes.value(parser.map());
            }
            expected += replacedEstimate(type, mapContent, size);
        }
        try (ParsedContentReservation reservation = new ParsedContentReservation(requestBreaker(ByteSizeValue.ofMb(100)), true)) {
            try (XContentParser parser = reservation.track(parser(searchRegistry(), type, content))) {
                new SearchSourceBuilder().parseXContent(parser, true, feature -> true);
            }
            assertThat(reservation.reservedBytes(), equalTo(expected));
        }
    }

    /**
     * What replacing the estimate for the tokens of {@code section} with {@code size} changes in the charge. The size must be below the
     * estimate, so that keeping the estimate fails the test.
     */
    private static long replacedEstimate(XContentType type, BytesReference section, long size) throws IOException {
        long estimate = charge(type, section, ParsedContentReservationTests::readAllTokens) - distinctNames(type, section)
            - ChargingXContentParser.TOKEN_BYTES;
        assertThat(size, greaterThan(0L));
        assertThat(size, lessThan(estimate));
        return size - estimate;
    }

    private static BytesReference document(XContentType type, Map<String, Object> map) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            return BytesReference.bytes(builder.map(map));
        }
    }

    /**
     * What {@code {"outer": {"<first>": {...}, "sized": {...}}}} is charged when {@code outer} reports {@code outerSize}, {@code sized}
     * reports {@code innerSize}, and the parser of {@code outer} catches what fails in {@code first} and goes on. The names for
     * {@code first} have the same length, so their tokens cost the same.
     */
    private static long chargeWithFirstInner(XContentType type, String first, long outerSize, long innerSize) throws IOException {
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject("outer");
            builder.startObject(first).field("key", "value").endObject();
            builder.startObject("sized").field("key", "value").endObject();
            builder.endObject().endObject();
            content = BytesReference.bytes(builder);
        }
        NamedXContentRegistry registry = new NamedXContentRegistry(
            List.of(new NamedXContentRegistry.Entry(Object.class, new ParseField("outer"), p -> {
                for (XContentParser.Token token = p.nextToken(); token != XContentParser.Token.END_OBJECT; token = p.nextToken()) {
                    String name = p.currentName();
                    assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                    try {
                        p.namedObject(Object.class, name, null);
                    } catch (IllegalStateException e) {
                        // Go on with the next object.
                    }
                }
                return new Sized(outerSize, null);
            }),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("sized"), p -> new Sized(innerSize, p.map())),
                new NamedXContentRegistry.Entry(
                    Object.class,
                    new ParseField("unsized"),
                    p -> new Sized(SizedParsedObject.UNKNOWN, p.map())
                ),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("failing"), p -> {
                    p.map();
                    throw new IllegalStateException("fails once its tokens are read");
                }),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("badsize"), p -> new FailsToSize(p.map()))
            )
        );
        try (ParsedContentReservation reservation = new ParsedContentReservation(requestBreaker(ByteSizeValue.ofGb(10)), true)) {
            try (XContentParser parser = reservation.track(parser(registry, type, content))) {
                readNamedObjectDocument(parser);
            }
            return reservation.reservedBytes();
        }
    }

    /** An object that throws when asked for its size. */
    private record FailsToSize(Object content) implements SizedParsedObject {
        @Override
        public long parsedRetainedBytes() {
            throw new IllegalStateException("cannot be sized");
        }
    }

    private static NamedXContentRegistry searchRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }

    /** {@code {"<name>": <body>}} */
    private static BytesReference namedObjectDocument(XContentType type, String name, BytesReference body) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().field(name);
            try (XContentParser parser = parser(type, body)) {
                builder.copyCurrentStructure(parser);
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /** What a tracked {@link #namedObjectParser} charges to read {@code content}. */
    private static long chargeOfNamedObject(XContentType type, BytesReference content, long outerSize, long innerSize) throws IOException {
        try (ParsedContentReservation reservation = new ParsedContentReservation(requestBreaker(ByteSizeValue.ofGb(10)), true)) {
            try (XContentParser parser = reservation.track(namedObjectParser(type, content, outerSize, innerSize))) {
                readNamedObjectDocument(parser);
            }
            return reservation.reservedBytes();
        }
    }

    private static void readNamedObjectDocument(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        String name = parser.nextFieldName();
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        parser.namedObject(Object.class, name, null);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertNull(parser.nextToken());
    }

    /**
     * A parser whose registry parses {@code sized} as a map that reports {@code outerSize}, and {@code outer} as an object that reports
     * {@code outerSize} and parses each of its fields as an {@code inner} map that reports {@code innerSize}.
     */
    private static XContentParser namedObjectParser(XContentType type, BytesReference content, long outerSize, long innerSize)
        throws IOException {
        NamedXContentRegistry registry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(Object.class, new ParseField("sized"), p -> new Sized(outerSize, p.map())),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("inner"), p -> new Sized(innerSize, p.map())),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("outer"), p -> {
                    for (XContentParser.Token token = p.nextToken(); token != XContentParser.Token.END_OBJECT; token = p.nextToken()) {
                        assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
                        assertThat(p.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                        p.namedObject(Object.class, "inner", null);
                    }
                    return new Sized(outerSize, null);
                })
            )
        );
        return parser(registry, type, content);
    }

    /** An object that reports {@code size}, which can be {@link SizedParsedObject#UNKNOWN}. */
    private record Sized(long size, Object content) implements SizedParsedObject {
        @Override
        public long parsedRetainedBytes() {
            return size;
        }
    }

    private static CircuitBreaker requestBreaker(ByteSizeValue limit) {
        HierarchyCircuitBreakerService service = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
                // Only the request breaker's own limit, so the results do not depend on the test JVM's heap.
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        return service.getBreaker(CircuitBreaker.REQUEST);
    }

    /** What a tracked parser charges once {@code reader} has read {@code content} and the parser is closed. */
    private static long charge(XContentType type, BytesReference content, CheckedFunction<XContentParser, ?, IOException> reader)
        throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofMb(100));
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, true)) {
            try (XContentParser parser = reservation.track(parser(type, content))) {
                reader.apply(parser);
            }
            return reservation.reservedBytes();
        }
    }

    /**
     * The charge for {@code content}, worked out from the tokens of an untracked parser. Field-name strings are charged the way the tracked
     * parser charges them, told apart by identity.
     */
    private static long expectedCharge(XContentType type, BytesReference content) throws IOException {
        long expected = 0;
        Set<String> names = Collections.newSetFromMap(new IdentityHashMap<>());
        try (XContentParser parser = parser(type, content)) {
            for (XContentParser.Token token = parser.nextToken(); token != null; token = parser.nextToken()) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    expected += ChargingXContentParser.STRING_VALUE_BYTES + ChargingXContentParser.BYTES_PER_CHAR * parser.text().length();
                } else if (token == XContentParser.Token.FIELD_NAME) {
                    String name = parser.currentName();
                    expected += ChargingXContentParser.TOKEN_BYTES + ChargingXContentParser.BYTES_PER_CHAR * name.length();
                    if (names.contains(name) == false) {
                        if (names.size() < ChargingXContentParser.MAX_CHARGED_NAMES) {
                            names.add(name);
                        }
                        expected += ParsedObjectSizes.string(name);
                    }
                } else {
                    expected += ChargingXContentParser.TOKEN_BYTES;
                }
            }
        }
        return expected;
    }

    /** What the strings of the distinct field names of {@code content} are charged, once each. */
    private static long distinctNames(XContentType type, BytesReference content) throws IOException {
        long charged = 0;
        Set<String> names = Collections.newSetFromMap(new IdentityHashMap<>());
        try (XContentParser parser = parser(type, content)) {
            for (XContentParser.Token token = parser.nextToken(); token != null; token = parser.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME && names.add(parser.currentName())) {
                    charged += ParsedObjectSizes.string(parser.currentName());
                }
            }
        }
        return charged;
    }

    private static long countTokens(XContentType type, BytesReference content) throws IOException {
        try (XContentParser parser = parser(type, content)) {
            return readAllTokens(parser);
        }
    }

    private static long readAllTokens(XContentParser parser) throws IOException {
        long tokens = 0;
        while (parser.nextToken() != null) {
            tokens++;
        }
        return tokens;
    }

    private static XContentParser parser(XContentType type, BytesReference content) throws IOException {
        return XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, content, type);
    }

    private static XContentParser parser(NamedXContentRegistry registry, XContentType type, BytesReference content) throws IOException {
        return XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY.withRegistry(registry), content, type);
    }

    /** A search-like document with a {@code bool} query of {@code clauses} {@code term} clauses and a few other kinds of values. */
    private static BytesReference document(XContentType type, int clauses) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject();
            builder.startObject("query").startObject("bool").startArray("should");
            for (int i = 0; i < clauses; i++) {
                builder.startObject().startObject("term").field("field", "value-" + i).endObject().endObject();
            }
            builder.endArray().endObject().endObject();
            builder.field("size", 10);
            builder.field("track_total_hits", true);
            builder.nullField("post_filter");
            builder.array("stored_fields", "a", "bb", "ccc");
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private static BytesReference stringMap(XContentType type, int entries) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject();
            for (int i = 0; i < entries; i++) {
                builder.field("key-" + i, "value-" + i);
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private static BytesReference array(XContentType type, int elements) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startArray();
            for (int i = 0; i < elements; i++) {
                if (i % 2 == 0) {
                    builder.value("value-" + i);
                } else {
                    builder.startObject().field("key", "value-" + i).field("n", i).endObject();
                }
            }
            builder.endArray();
            return BytesReference.bytes(builder);
        }
    }
}
