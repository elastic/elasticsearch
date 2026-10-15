/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;

/** Checks EQL's canonical corpus, an independent history-based oracle, and request-memory ownership. */
public class SequenceMatcherTests extends ESTestCase {
    private static final Comparator<Event> EVENT_ORDER = Comparator.comparing(Event::ordinal)
        .thenComparing(Comparator.comparingInt(Event::stage).reversed())
        .thenComparing(Event::key);

    public void testEqlCanonicalCorpus() throws Exception {
        // Gradle copies the original resource directly; changes to EQL's expected matches reach this test too.
        try (
            var input = getClass().getResourceAsStream("/sequence/sequences.series-spec");
            var reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))
        ) {
            String name = null;
            List<List<String>> stageTokens = new ArrayList<>();
            List<String> expected = new ArrayList<>();
            boolean readingResults = false;
            int checked = 0;
            for (String line; (line = reader.readLine()) != null;) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("//")) {
                    continue;
                }
                if (name == null) {
                    name = line;
                } else if (line.equals(";")) {
                    if (readingResults == false) {
                        readingResults = true;
                    } else {
                        List<Event> events = new ArrayList<>();
                        for (int stage = 0; stage < stageTokens.size(); stage++) {
                            for (String token : stageTokens.get(stage)) {
                                int pipe = token.indexOf('|');
                                String key = pipe < 0 ? "" : token.substring(0, pipe);
                                String handle = token.substring(pipe + 1);
                                int timestamp = Integer.parseInt(handle.replaceAll("[^0-9]", ""));
                                events.add(new Event(stage, key, new SequenceOrdinal(timestamp, 0, timestamp), handle));
                            }
                        }
                        assertEquals(name, expected, run(stageTokens.size(), TimeValue.timeValueDays(1), events));
                        checked++;
                        name = null;
                        stageTokens.clear();
                        expected.clear();
                        readingResults = false;
                    }
                } else if (readingResults) {
                    expected.add(line.replaceAll("\\s+", " "));
                } else {
                    stageTokens.add(List.of(line.split("\\s+")));
                }
            }
            assertNull("incomplete EQL corpus", name);
            assertEquals("every EQL positive-sequence fixture must run", 25, checked);
        }
    }

    public void testRandomizedHistoryOracle() {
        for (int iteration = 0; iteration < 100; iteration++) {
            int stages = between(2, 8);
            TimeValue maxSpan = TimeValue.timeValueNanos(between(1, 10) * 1_000_000L + between(0, 999_999));
            List<Event> events = new ArrayList<>();
            long timestamp = 0;
            for (int event = 0; event < 100; event++) {
                timestamp += between(0, 2);
                SequenceOrdinal ordinal = new SequenceOrdinal(timestamp, between(0, 999_999), event);
                for (int stage = 0; stage < stages; stage++) {
                    if (randomBoolean()) {
                        String key = Integer.toString(between(0, 3));
                        Event row = new Event(stage, key, ordinal, "event" + event);
                        events.add(row);
                        if (randomBoolean()) {
                            events.add(row); // Repeated expanded key, retaining source identity.
                        }
                    }
                }
            }
            events.sort(EVENT_ORDER);
            assertEquals("iteration " + iteration, historyOracle(stages, maxSpan, events), run(stages, maxSpan, events));
        }
    }

    public void testOverlappingPredicatesKeepStrictSourceOrder() {
        SequenceOrdinal first = new SequenceOrdinal(0, 0, 1);
        SequenceOrdinal second = new SequenceOrdinal(0, 0, 2);
        SequenceOrdinal third = new SequenceOrdinal(0, 0, 3);
        assertEquals(
            List.of("k|first second", "k|second third"),
            run(
                2,
                TimeValue.timeValueSeconds(1),
                List.of(
                    new Event(0, "k", first, "first"),
                    new Event(1, "k", first, "first"),
                    new Event(0, "k", second, "second"),
                    new Event(1, "k", second, "second"),
                    new Event(1, "k", third, "third")
                )
            )
        );
    }

    public void testInclusiveTotalMaxspanAtNanosecondPrecision() {
        SequenceOrdinal start = new SequenceOrdinal(100, 999_999, 1);
        TimeValue span = TimeValue.timeValueNanos(2);
        assertEquals(
            List.of("A B"),
            run(2, span, List.of(new Event(0, "", start, "A"), new Event(1, "", new SequenceOrdinal(101, 1, 2), "B")))
        );
        assertEquals(List.of(), run(2, span, List.of(new Event(0, "", start, "A"), new Event(1, "", new SequenceOrdinal(101, 2, 2), "B"))));
        assertEquals(
            List.of(),
            run(
                3,
                TimeValue.timeValueMillis(5),
                List.of(
                    new Event(0, "", new SequenceOrdinal(0, 0, 1), "A"),
                    new Event(1, "", new SequenceOrdinal(4, 0, 2), "B"),
                    new Event(2, "", new SequenceOrdinal(8, 0, 3), "C")
                )
            )
        );
    }

    public void testWideDateRangeDoesNotOverflowSpan() {
        assertEquals(
            List.of(),
            run(
                2,
                TimeValue.timeValueSeconds(30),
                List.of(
                    new Event(0, "", new SequenceOrdinal(Long.MIN_VALUE, 0, 1), "A"),
                    new Event(1, "", new SequenceOrdinal(Long.MAX_VALUE, 0, 2), "B")
                )
            )
        );
        SequenceOrdinal beforeEpoch = new SequenceOrdinal(-1, 999_999, 1);
        assertFalse(new SequenceOrdinal(0, 0, 2).exceedsSpan(beforeEpoch, 1));
    }

    public void testHotKeyStateDoesNotGrowWithCandidateCount() {
        CircuitBreaker breaker = requestBreaker("10mb");
        try (SequenceMatcher matcher = matcher(3, TimeValue.timeValueDays(1), breaker)) {
            assertNull(matcher.match(0, new BytesRef("k"), new SequenceOrdinal(0, 0, 0), new BytesRef("A")));
            long anchorBytes = matcher.ramBytesUsed();
            for (int i = 1; i < 10_000; i++) {
                assertNull(matcher.match(0, new BytesRef("k"), new SequenceOrdinal(i, 0, i), new BytesRef("A")));
                assertEquals(1, matcher.pendingCandidates());
                assertEquals(anchorBytes, matcher.ramBytesUsed());
            }
            assertNull(matcher.match(1, new BytesRef("k"), new SequenceOrdinal(10_000, 0, 10_000), new BytesRef("B")));
            try (
                SequenceMatcher.Match match = matcher.match(2, new BytesRef("k"), new SequenceOrdinal(10_001, 0, 10_001), new BytesRef("C"))
            ) {
                assertEquals(List.of(new BytesRef("A"), new BytesRef("B"), new BytesRef("C")), match.handles());
                assertEquals(new SequenceOrdinal(9_999, 0, 9_999), match.start());
            }
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testWatermarkPrunesUnrelatedKeys() {
        CircuitBreaker breaker = requestBreaker("10mb");
        try (SequenceMatcher matcher = matcher(2, TimeValue.timeValueMillis(1), breaker)) {
            for (int i = 0; i < 1000; i++) {
                assertNull(matcher.match(0, new BytesRef("key" + i), new SequenceOrdinal(0, 0, i), new BytesRef("A")));
            }
            assertEquals(1000, matcher.pendingCandidates());
            long peak = breaker.getUsed();
            assertNull(matcher.match(1, new BytesRef("unrelated"), new SequenceOrdinal(2, 0, 1001), new BytesRef("B")));
            assertEquals(0, matcher.pendingCandidates());
            assertThat(breaker.getUsed(), lessThan(peak / 4));
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testOutputOwnsItsReservationAfterMatcherClose() {
        CircuitBreaker breaker = requestBreaker("10mb");
        SequenceMatcher matcher = matcher(2, TimeValue.timeValueSeconds(1), breaker);
        BytesRef key = new BytesRef("k");
        BytesRef handle = new BytesRef("A");
        assertNull(matcher.match(0, key, new SequenceOrdinal(0, 0, 1), handle));
        key.bytes[key.offset] = 'x';
        handle.bytes[handle.offset] = 'X';
        SequenceMatcher.Match match = matcher.match(1, new BytesRef("k"), new SequenceOrdinal(1, 0, 2), new BytesRef("B"));
        assertNotNull(match);
        try (match) {
            matcher.close();
            matcher.close();
            assertTrue(breaker.getUsed() > 0);
            assertEquals(new BytesRef("k"), match.key());
            assertEquals(List.of(new BytesRef("A"), new BytesRef("B")), match.handles());
        } finally {
            matcher.close();
        }
        match.close();
        assertEquals(0, breaker.getUsed());
        expectThrows(IllegalStateException.class, match::handles);
    }

    public void testBreakerTripsReleasePendingAndTransientOutput() {
        for (int failingStage = 0; failingStage < 3; failingStage++) {
            CircuitBreaker breaker = requestBreaker("10kb");
            try (SequenceMatcher matcher = matcher(3, TimeValue.timeValueSeconds(1), breaker)) {
                for (int stage = 0; stage < failingStage; stage++) {
                    assertNull(matcher.match(stage, new BytesRef("k"), new SequenceOrdinal(stage, 0, stage), new BytesRef("small")));
                }
                int stage = failingStage;
                expectThrows(
                    CircuitBreakingException.class,
                    () -> matcher.match(stage, new BytesRef("k"), new SequenceOrdinal(stage, 0, stage), new BytesRef(new byte[20_000]))
                );
                assertEquals(0, breaker.getUsed());
            }
        }
    }

    public void testBreakerTripsOnGrowingKeySpace() {
        CircuitBreaker breaker = requestBreaker("32kb");
        try (SequenceMatcher matcher = matcher(2, TimeValue.timeValueSeconds(1), breaker)) {
            expectThrows(CircuitBreakingException.class, () -> {
                for (int i = 0; i < 10_000; i++) {
                    matcher.match(0, new BytesRef("key" + i), new SequenceOrdinal(0, 0, i), new BytesRef("A"));
                }
            });
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testCancellationReleasesState() {
        CircuitBreaker breaker = requestBreaker("10mb");
        AtomicBoolean cancelled = new AtomicBoolean();
        try (SequenceMatcher matcher = new SequenceMatcher(2, TimeValue.timeValueSeconds(1), breaker, cancelled::get)) {
            matcher.match(0, new BytesRef("k"), new SequenceOrdinal(0, 0, 1), new BytesRef("A"));
            cancelled.set(true);
            expectThrows(
                TaskCancelledException.class,
                () -> matcher.match(1, new BytesRef("k"), new SequenceOrdinal(1, 0, 2), new BytesRef("B"))
            );
            assertEquals(0, breaker.getUsed());
        }
    }

    public void testUnorderedInputsFailAndReleaseState() {
        CircuitBreaker breaker = requestBreaker("10mb");
        try (SequenceMatcher matcher = matcher(2, TimeValue.timeValueSeconds(1), breaker)) {
            matcher.match(0, new BytesRef("k"), new SequenceOrdinal(0, 0, 1), new BytesRef("A"));
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> matcher.match(1, new BytesRef("k"), new SequenceOrdinal(0, 0, 1), new BytesRef("B"))
            );
            assertThat(error.getMessage(), containsString("descending stage"));
            assertEquals(0, breaker.getUsed());
            expectThrows(
                IllegalStateException.class,
                () -> matcher.match(0, new BytesRef("k"), new SequenceOrdinal(1, 0, 2), new BytesRef("A"))
            );
        }
    }

    public void testInvalidConstructionAndOrdinal() {
        CircuitBreaker breaker = requestBreaker("10mb");
        expectThrows(IllegalArgumentException.class, () -> matcher(1, TimeValue.timeValueSeconds(1), breaker));
        expectThrows(IllegalArgumentException.class, () -> matcher(2, TimeValue.ZERO, breaker));
        expectThrows(IllegalArgumentException.class, () -> new SequenceOrdinal(0, -1, 0));
        expectThrows(IllegalArgumentException.class, () -> new SequenceOrdinal(0, 1_000_000, 0));
        expectThrows(IllegalArgumentException.class, () -> new SequenceOrdinal(0, 0, -1));
        assertEquals(0, breaker.getUsed());
    }

    private static SequenceMatcher matcher(int stages, TimeValue span, CircuitBreaker breaker) {
        return new SequenceMatcher(stages, span, breaker, () -> false);
    }

    private static List<String> run(int stages, TimeValue span, List<Event> input) {
        CircuitBreaker breaker = requestBreaker("10mb");
        List<Event> events = new ArrayList<>(input);
        events.sort(EVENT_ORDER);
        List<String> result = new ArrayList<>();
        try (SequenceMatcher matcher = matcher(stages, span, breaker)) {
            for (Event event : events) {
                try (
                    SequenceMatcher.Match match = matcher.match(
                        event.stage,
                        new BytesRef(event.key),
                        event.ordinal,
                        new BytesRef(event.handle)
                    )
                ) {
                    if (match != null) {
                        result.add(
                            (event.key.isEmpty() ? "" : event.key + "|") + String.join(
                                " ",
                                match.handles().stream().map(BytesRef::utf8ToString).toList()
                            )
                        );
                    }
                }
            }
        }
        assertEquals(0, breaker.getUsed());
        return result;
    }

    /** Retains all prefixes and consumes the latest strict predecessor as EQL does, without the compact-state optimization. */
    private static List<String> historyOracle(int stages, TimeValue span, List<Event> events) {
        List<List<Prefix>> pending = new ArrayList<>();
        for (int stage = 0; stage < stages - 1; stage++) {
            pending.add(new ArrayList<>());
        }
        List<String> results = new ArrayList<>();
        for (Event event : events) {
            Prefix prefix;
            if (event.stage == 0) {
                pending.get(0).add(new Prefix(event.key, event.ordinal, event.ordinal, List.of(event.handle)));
                continue;
            }
            List<Prefix> previous = pending.get(event.stage - 1);
            prefix = null;
            for (Prefix candidate : previous) {
                if (candidate.key.equals(event.key) && candidate.end.compareTo(event.ordinal) < 0) {
                    prefix = candidate;
                }
            }
            previous.removeIf(candidate -> candidate.key.equals(event.key) && candidate.end.compareTo(event.ordinal) < 0);
            if (prefix == null
                || epochNanos(event.ordinal).subtract(epochNanos(prefix.start)).compareTo(BigInteger.valueOf(span.nanos())) > 0) {
                continue;
            }
            List<String> handles = new ArrayList<>(prefix.handles);
            handles.add(event.handle);
            if (event.stage == stages - 1) {
                results.add(event.key + "|" + String.join(" ", handles));
            } else {
                pending.get(event.stage).add(new Prefix(event.key, prefix.start, event.ordinal, handles));
            }
        }
        return results;
    }

    private static BigInteger epochNanos(SequenceOrdinal ordinal) {
        return BigInteger.valueOf(ordinal.epochMillis())
            .multiply(BigInteger.valueOf(1_000_000))
            .add(BigInteger.valueOf(ordinal.nanoOfMilli()));
    }

    private record Prefix(String key, SequenceOrdinal start, SequenceOrdinal end, List<String> handles) {}

    private record Event(int stage, String key, SequenceOrdinal ordinal, String handle) {}

    private static CircuitBreaker requestBreaker(String limit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), limit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ).getBreaker(CircuitBreaker.REQUEST);
    }
}
