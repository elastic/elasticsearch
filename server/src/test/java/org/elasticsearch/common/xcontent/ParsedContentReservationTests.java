/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.breaker.ChildMemoryCircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.MeterRegistry;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests that a tracked parser charges for every token however it is read, that the breaker is checked each time a request charges a
 * chunk, and that the charge is held until the reservation closes and no hold remains.
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

    public void testBigDecimalIsChargedByItsDigits() throws IOException {
        // JSON reads a decimal as a double, so only the binary formats keep a BigDecimal.
        XContentType type = randomFrom(XContentType.SMILE, XContentType.CBOR);
        BigDecimal value = new BigDecimal("1234567890.12345678901234567890");
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            content = BytesReference.bytes(builder.startObject().field("value", value).endObject());
        }
        long charged = charge(type, content, ParsedContentReservationTests::readAllTokens);
        try (XContentParser parser = parser(type, content)) {
            parser.nextToken();
            parser.nextToken();
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
            assertThat(parser.numberType(), equalTo(XContentParser.NumberType.BIG_DECIMAL));
        }
        assertThat(charged, equalTo(expectedCharge(type, content)));
        assertThat(charged, greaterThan(expectedCharge(type, numberDocument(type, 1.5))));
    }

    private static BytesReference numberDocument(XContentType type, double value) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            return BytesReference.bytes(builder.startObject().field("value", value).endObject());
        }
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

    public void testHeldMemoryMetricShowsTheChargeUnderItsCategory() throws IOException {
        RecordingMeterRegistry meter = new RecordingMeterRegistry();
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofMb(100), meter);
        XContentType type = randomFrom(TYPES);
        BytesReference content = document(type, 20_000);
        try (ParsedContentReservation reservation = new ParsedContentReservation(breaker, randomBoolean())) {
            try (XContentParser parser = reservation.track(parser(type, content))) {
                readAllTokens(parser);
            }
            assertThat(reservation.reservedBytes(), greaterThan(0L));
            assertThat(heldMemory(meter, ChildMemoryCircuitBreaker.CATEGORY_PARSED_REQUEST), equalTo(reservation.reservedBytes()));
            assertThat(heldMemory(meter, ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED), equalTo(0L));
        }
        // Released under the same category, so it does not stay held there.
        assertThat(heldMemory(meter, ChildMemoryCircuitBreaker.CATEGORY_PARSED_REQUEST), equalTo(0L));
        assertThat(heldMemory(meter, ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED), equalTo(0L));
    }

    public void testChargeThatRacesWithCloseIsReleasedUnderItsCategory() {
        RecordingMeterRegistry meter = new RecordingMeterRegistry();
        CircuitBreakerMetrics metrics = metrics(meter);
        ByteSizeValue limit = ByteSizeValue.ofMb(100);
        HierarchyCircuitBreakerService service = requestBreakerService(limit, metrics);
        AtomicReference<ParsedContentReservation> reservation = new AtomicReference<>();
        // A breaker of its own, since the service's request breaker cannot close the reservation. Named request so its metrics match
        // the ones read below. Its usage is not counted by the service's parent limit.
        ChildMemoryCircuitBreaker breaker = new ChildMemoryCircuitBreaker(
            metrics,
            new BreakerSettings(CircuitBreaker.REQUEST, limit.getBytes(), 1.0),
            logger,
            service,
            CircuitBreaker.REQUEST
        ) {
            @Override
            public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
                super.addEstimateBytesAndMaybeBreak(bytes, label);
                closeReservation();
            }

            @Override
            public void addWithoutBreaking(long bytes, String label) {
                super.addWithoutBreaking(bytes, label);
                if (bytes > 0) {
                    closeReservation();
                }
            }

            // Simulates the request completing while a forked parser is still charging.
            private void closeReservation() {
                ParsedContentReservation toClose = reservation.getAndSet(null);
                if (toClose != null) {
                    toClose.close();
                }
            }
        };
        for (boolean checked : new boolean[] { true, false }) {
            ParsedContentReservation racing = new ParsedContentReservation(breaker, true);
            reservation.set(racing);
            long bytes = between(1, 10_000);
            if (checked) {
                racing.reserve(bytes);
            } else {
                racing.reserveWithoutBreaking(bytes);
            }
            assertThat(racing.reservedBytes(), equalTo(0L));
            assertThat(breaker.getUsed(), equalTo(0L));
            // Released under the category it was charged under, so neither stays held or goes negative.
            assertThat(heldMemory(meter, ChildMemoryCircuitBreaker.CATEGORY_PARSED_REQUEST), equalTo(0L));
            assertThat(heldMemory(meter, ChildMemoryCircuitBreaker.CATEGORY_UNCATEGORIZED), equalTo(0L));
        }
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

    public void testChargeIsHeldUntilTheRequestAndEveryHoldAreReleased() throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofMb(100));
        XContentType type = randomFrom(TYPES);
        ParsedContentReservation reservation = new ParsedContentReservation(breaker, true);
        try (XContentParser parser = reservation.track(parser(type, document(type, between(2, 100))))) {
            readAllTokens(parser);
        }
        long charged = reservation.reservedBytes();
        assertThat(charged, greaterThan(0L));
        // The request responds, closing the reservation, before or after the work that keeps the parsed request releases its holds.
        List<Releasable> releases = new ArrayList<>();
        releases.add(reservation);
        for (int i = between(1, 3); i > 0; i--) {
            releases.add(reservation.hold());
        }
        Collections.shuffle(releases, random());
        for (Releasable release : releases.subList(0, releases.size() - 1)) {
            release.close();
            // Closing twice does not drop another hold.
            release.close();
            assertThat(breaker.getUsed(), equalTo(charged));
        }
        // Content parsed for the request while it is held is charged too.
        try (XContentParser parser = reservation.track(parser(type, document(type, 1)))) {
            readAllTokens(parser);
        }
        long chargedWhileHeld = reservation.reservedBytes();
        assertThat(chargedWhileHeld, greaterThan(charged));
        assertThat(breaker.getUsed(), equalTo(chargedWhileHeld));
        releases.getLast().close();
        assertThat(reservation.reservedBytes(), equalTo(0L));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testHoldAfterTheChargeIsReleasedDoesNothing() throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofMb(100));
        XContentType type = randomFrom(TYPES);
        ParsedContentReservation reservation = new ParsedContentReservation(breaker, true);
        reservation.close();
        try (Releasable ignored = reservation.hold()) {
            try (XContentParser parser = reservation.track(parser(type, document(type, between(2, 100))))) {
                readAllTokens(parser);
            }
            assertThat(reservation.reservedBytes(), equalTo(0L));
            assertThat(breaker.getUsed(), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testHoldsTheReservationInTheThreadContextIfAny() throws IOException {
        CircuitBreaker breaker = requestBreaker(ByteSizeValue.ofMb(100));
        XContentType type = randomFrom(TYPES);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        // No request is handled, so there is nothing to hold.
        ParsedContentReservation.hold(threadContext).close();
        ParsedContentReservation reservation = new ParsedContentReservation(breaker, true);
        threadContext.putTransient(ParsedContentReservation.TRANSIENT_NAME, reservation);
        try (XContentParser parser = reservation.track(parser(type, document(type, between(2, 100))))) {
            readAllTokens(parser);
        }
        long charged = reservation.reservedBytes();
        Releasable hold = ParsedContentReservation.hold(threadContext);
        reservation.close();
        assertThat(breaker.getUsed(), equalTo(charged));
        hold.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testParserTripsAgainAfterACaughtTrip() throws IOException {
        XContentType type = randomFrom(TYPES);
        BytesReference content;
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject().startObject("o1").startObject("o2");
            // Charged just over a chunk, above the limit.
            builder.field("s", "x".repeat(1_050_000));
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
                    return "o1";
                }), new NamedXContentRegistry.Entry(Object.class, new ParseField("o2"), XContentParser::map))
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

    public void testQueryParserReadsThroughTrackedParser() throws IOException {
        XContentType type = randomFrom(TYPES);
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
            try (XContentParser parser = reservation.track(parser(searchRegistry(), type, content))) {
                // parseTopLevelQuery parses named objects through its own wrapper, which must still read through the tracked parser.
                AbstractQueryBuilder.parseTopLevelQuery(parser);
            }
            assertThat(reservation.reservedBytes(), equalTo(expectedCharge(type, content)));
        }
    }

    private static NamedXContentRegistry searchRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }

    private static void readNamedObjectDocument(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        String name = parser.nextFieldName();
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        parser.namedObject(Object.class, name, null);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertNull(parser.nextToken());
    }

    private static CircuitBreaker requestBreaker(ByteSizeValue limit) {
        return requestBreakerService(limit, CircuitBreakerMetrics.NOOP).getBreaker(CircuitBreaker.REQUEST);
    }

    /** A request breaker that records its metrics in {@code meter}. */
    private static CircuitBreaker requestBreaker(ByteSizeValue limit, RecordingMeterRegistry meter) {
        return requestBreakerService(limit, metrics(meter)).getBreaker(CircuitBreaker.REQUEST);
    }

    private static CircuitBreakerMetrics metrics(RecordingMeterRegistry meter) {
        return new CircuitBreakerMetrics(new TelemetryProvider.NoopTelemetryProvider() {
            @Override
            public MeterRegistry getMeterRegistry() {
                return meter;
            }
        });
    }

    private static HierarchyCircuitBreakerService requestBreakerService(ByteSizeValue limit, CircuitBreakerMetrics metrics) {
        return new HierarchyCircuitBreakerService(
            metrics,
            Settings.builder()
                .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
                // Only the request breaker's own limit, so the results do not depend on the test JVM's heap.
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .build(),
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    /** The request breaker's {@code es.breaker.memory.held.usage} under {@code category}. */
    private static long heldMemory(RecordingMeterRegistry meter, String category) {
        return meter.getRecorder()
            .getMeasurements(InstrumentType.LONG_UP_DOWN_COUNTER, CircuitBreakerMetrics.ES_BREAKER_MEMORY_HELD)
            .stream()
            .filter(m -> CircuitBreaker.REQUEST.equals(m.attributes().get(ChildMemoryCircuitBreaker.BREAKER_METRIC_TYPE_ATTRIBUTE)))
            .filter(m -> category.equals(m.attributes().get(ChildMemoryCircuitBreaker.CIRCUIT_BREAKER_CATEGORY_ATTRIBUTE)))
            .mapToLong(Measurement::getLong)
            .sum();
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
                    expected += ChargingXContentParser.STRING_VALUE_BYTES + valueBytes(parser.text());
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    expected += switch (parser.numberType()) {
                        case INT, LONG, FLOAT, DOUBLE -> ChargingXContentParser.NUMBER_VALUE_BYTES;
                        case BIG_INTEGER, BIG_DECIMAL -> ChargingXContentParser.STRING_VALUE_BYTES + valueBytes(parser.text());
                    };
                } else if (token == XContentParser.Token.FIELD_NAME) {
                    String name = parser.currentName();
                    expected += ChargingXContentParser.TOKEN_BYTES + ChargingXContentParser.NAME_BYTES_PER_CHAR * name.length();
                    if (names.contains(name) == false) {
                        if (names.size() < ChargingXContentParser.MAX_CHARGED_NAMES) {
                            names.add(name);
                        }
                        expected += RamUsageEstimator.sizeOf(name);
                    }
                } else {
                    expected += ChargingXContentParser.TOKEN_BYTES;
                }
            }
        }
        return expected;
    }

    /** What a string value is charged for its text: its UTF-8 length, or two bytes a character once any is outside Latin-1. */
    private static long valueBytes(String text) {
        long utf8 = text.getBytes(StandardCharsets.UTF_8).length;
        return text.chars().allMatch(c -> c <= 0xFF) ? utf8 : Math.max(utf8, 2L * text.length());
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
            builder.field("min_score", 0.5);
            builder.field("terminate_after", 10_000_000_000L);
            // Too large for a long, so charged by its digits.
            builder.field("big", new BigInteger("123456789012345678901234567890"));
            // Charged as a String, which is larger than the UTF-8 length once any character is outside Latin-1.
            builder.field("mixed", "abc caf\u00e9 \u65e5\u672c \ud83d\ude00");
            // Charged the UTF-8 length, which is larger than a String for CJK and for Latin-1 with accents, and the same for ASCII.
            builder.field("cjk", "\u65e5\u672c\u8a9e");
            builder.field("latin1", "caf\u00e9");
            builder.field("ascii", "plain");
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
