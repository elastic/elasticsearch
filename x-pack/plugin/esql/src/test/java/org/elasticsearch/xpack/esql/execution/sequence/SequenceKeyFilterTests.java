/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;

/** Canonical key snapshots must retain tuple equality, bounded admission and independent ownership. */
public class SequenceKeyFilterTests extends ESTestCase {
    /** Integral precision and Unicode values must survive key decoding without display conversions. */
    public void testTypedValuesRoundTrip() {
        var breaker = breaker("1mb");
        List<Attribute> fields = List.of(field("number", DataType.LONG), field("text", DataType.KEYWORD), field("flag", DataType.BOOLEAN));
        String text = randomUnicodeOfLength(30) + "\u0000";
        try (var owner = SequenceKeyFilter.create(fields, fields, breaker, () -> {})) {
            assertNotNull(owner);
            assertTrue(owner.add(key(List.of(DataType.LONG, DataType.KEYWORD, DataType.BOOLEAN), 9_007_199_254_740_993L, text, true)));
            assertEquals(Set.of(List.of(9_007_199_254_740_993L, text, true)), tuples(owner.predicate()));
        }
        assertEquals(0, breaker.getUsed());
    }

    /** Complete tuple alternatives must not admit the Cartesian cross-pairs of their individual components. */
    public void testCompositeTuplesStayCorrelatedAndDeduplicated() {
        var breaker = breaker("1mb");
        List<Attribute> fields = List.of(field("first", DataType.KEYWORD), field("second", DataType.KEYWORD));
        List<DataType> types = List.of(DataType.KEYWORD, DataType.KEYWORD);
        try (var owner = SequenceKeyFilter.create(fields, fields, breaker, () -> {})) {
            assertNotNull(owner);
            assertTrue(owner.add(key(types, "left", "right")));
            assertTrue(owner.add(key(types, "right", "left")));
            assertTrue(owner.add(key(types, "left", "right")));
            assertEquals(Set.of(List.of("left", "right"), List.of("right", "left")), tuples(owner.predicate()));
        }
        assertEquals(0, breaker.getUsed());
    }

    /** A rejected 129th key invalidates the complete predicate rather than exposing the admitted prefix. */
    public void testTupleCutoffFallsBackWithoutPublishingPrefix() {
        var breaker = breaker("1mb");
        List<Attribute> fields = List.of(field("key", DataType.INTEGER));
        for (int count : List.of(128, 129)) {
            try (var owner = SequenceKeyFilter.create(fields, fields, breaker, () -> {})) {
                assertNotNull(owner);
                for (int key = 0; key < count; key++) {
                    assertEquals(key < 128, owner.add(key(List.of(DataType.INTEGER), key)));
                }
                if (count == 128) {
                    assertEquals(128, tuples(owner.predicate()).size());
                } else {
                    assertNull(owner.predicate());
                }
            }
            assertEquals(0, breaker.getUsed());
        }
    }

    /** Unreadable encodings disable all optional narrowing, including previously valid tuples. */
    public void testInvalidKeywordDoesNotPublishValidPrefix() {
        var breaker = breaker("1mb");
        List<Attribute> fields = List.of(field("key", DataType.KEYWORD));
        try (var owner = SequenceKeyFilter.create(fields, fields, breaker, () -> {})) {
            assertNotNull(owner);
            assertTrue(owner.add(key(List.of(DataType.KEYWORD), "valid")));
            assertTrue(owner.add(new BytesRef(new byte[] { 6, 0, 0, 0, 1, (byte) 0xFF })));
            assertNull(owner.predicate());
        }
        assertEquals(0, breaker.getUsed());
    }

    /** Names alone do not prove raw origin, and unsupported scalar types must remain unfiltered. */
    public void testComputedMissingAndUnsupportedKeysFallBack() {
        var breaker = breaker("1mb");
        Attribute raw = field("key", DataType.KEYWORD);
        Attribute computed = new ReferenceAttribute(Source.EMPTY, null, "key", DataType.KEYWORD);
        assertNull(SequenceKeyFilter.create(List.of(computed), List.of(raw), breaker, () -> {}));
        assertNull(SequenceKeyFilter.create(List.of(raw), List.of(), breaker, () -> {}));
        Attribute unsupported = field("point", DataType.GEO_POINT);
        assertNull(SequenceKeyFilter.create(List.of(unsupported), List.of(unsupported), breaker, () -> {}));
        assertNull(SequenceKeyFilter.create(List.of(), List.of(raw), breaker, () -> {}));
        assertEquals(0, breaker.getUsed());
    }

    /** The key of a real optimized URI_PARTS stage has no native field identity for the coordinator predicate. */
    public void testUriDerivedKeyFallsBackAtCoordinator() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
        var configuration = EsqlTestUtils.configuration(QueryPragmas.EMPTY);
        var mapping = Map.<String, EsField>of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "url",
            new EsField("url", DataType.KEYWORD, Map.of(), true, NONE)
        );
        var analyzer = EsqlTestUtils.analyzer()
            .configuration(configuration)
            .minimumTransportVersion(TransportVersion.current())
            .addIndex(new EsIndex("events", mapping, Map.of("events", new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
        var plan = analyzer.statement("""
            SEQUENCE BY parsed.domain WITHIN 30s PRIMARY 1
            (FROM events | URI_PARTS parsed=url), (FROM events | URI_PARTS parsed=url)
            | KEEP parsed.domain | LIMIT 10
            """);
        var optimizer = new LogicalPlanOptimizer(
            EsqlTestUtils.logicalOptimizerContext(configuration, FoldContext.small(), TransportVersion.current())
        );
        var sequence = optimizer.optimize(plan).collect(Sequence.class).getFirst();
        var later = (SequenceStage) sequence.children().getLast();
        var input = SequenceStageInput.selection(later, List.of());
        assertEquals(List.of("parsed.domain"), later.keys().stream().map(Attribute::name).toList());
        assertTrue(input.source(0).output().stream().anyMatch(attribute -> attribute.name().equals("url")));
        var breaker = breaker("1mb");
        try (var filter = SequenceKeyFilter.create(later.keys(), input.source(0).output(), breaker, () -> {})) {
            assertNull(filter);
        }
        assertEquals(0, breaker.getUsed());
    }

    /** Matcher storage can be released after the snapshot, and terminal callbacks may close the query owner repeatedly. */
    public void testSnapshotOwnsBytesAndRefundsOnce() {
        var breaker = breaker("1mb");
        List<Attribute> fields = List.of(field("key", DataType.INTEGER));
        var owner = SequenceKeyFilter.create(fields, fields, breaker, () -> {});
        assertNotNull(owner);
        try {
            BytesRef borrowed = key(List.of(DataType.INTEGER), 7);
            assertTrue(owner.add(borrowed));
            Arrays.fill(borrowed.bytes, (byte) 0);
            assertEquals(Set.of(List.of(7)), tuples(owner.predicate()));
            expectThrows(IllegalStateException.class, () -> owner.add(key(List.of(DataType.INTEGER), 8)));
            assertTrue(breaker.getUsed() > 0);
        } finally {
            owner.close();
        }
        owner.close();
        assertEquals(0, breaker.getUsed());
        expectThrows(IllegalStateException.class, owner::predicate);
    }

    /** Failed request admission must leave no half-published key and refund every successful earlier reservation. */
    public void testAdmissionFailureKeepsOwnerRefundable() {
        var breaker = breaker("20kb");
        List<Attribute> fields = List.of(field("key", DataType.KEYWORD));
        try (var owner = SequenceKeyFilter.create(fields, fields, breaker, () -> {})) {
            assertNotNull(owner);
            assertTrue(owner.add(key(List.of(DataType.KEYWORD), "valid")));
            long admitted = breaker.getUsed();
            expectThrows(CircuitBreakingException.class, () -> owner.add(key(List.of(DataType.KEYWORD), "z".repeat(4096))));
            assertEquals(admitted, breaker.getUsed());
            assertNull(owner.predicate());
        }
        assertEquals(0, breaker.getUsed());
        var tiny = breaker("1kb");
        expectThrows(CircuitBreakingException.class, () -> SequenceKeyFilter.create(fields, fields, tiny, () -> {}));
        assertEquals(0, tiny.getUsed());
    }

    /** Cancellation must abandon the entire borrowed snapshot and still refund its owned admissions. */
    public void testCancellationKeepsSnapshotRefundable() {
        var breaker = breaker("1mb");
        List<Attribute> fields = List.of(field("key", DataType.INTEGER));
        AtomicBoolean cancelled = new AtomicBoolean();
        try (var owner = SequenceKeyFilter.create(fields, fields, breaker, () -> {
            if (cancelled.get()) {
                throw new IllegalStateException("cancelled key snapshot");
            }
        })) {
            assertNotNull(owner);
            assertTrue(owner.add(key(List.of(DataType.INTEGER), 1)));
            cancelled.set(true);
            assertEquals("cancelled key snapshot", expectThrows(IllegalStateException.class, owner::predicate).getMessage());
        }
        assertEquals(0, breaker.getUsed());
    }

    public void testEnabledByDefaultAndExplicitSettingsAreHonored() {
        assertTrue(new QueryPragmas(Settings.EMPTY).sequenceKeyFilter());
        assertFalse(new QueryPragmas(Settings.builder().put("sequence_key_filter", false).build()).sequenceKeyFilter());
        assertTrue(new QueryPragmas(Settings.builder().put("sequence_key_filter", true).build()).sequenceKeyFilter());
    }

    private static Attribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, NONE));
    }

    private static Set<List<Object>> tuples(QueryBuilder query) {
        if ((query instanceof BoolQueryBuilder) == false) {
            throw new AssertionError("Expected complete tuple alternatives");
        }
        // The explicit locals below follow the same type checks and preserve their exact assertion failures.
        BoolQueryBuilder alternatives = (BoolQueryBuilder) query;
        Set<List<Object>> rows = new HashSet<>();
        for (QueryBuilder alternative : alternatives.should()) {
            if ((alternative instanceof BoolQueryBuilder) == false) {
                throw new AssertionError("Expected an AND tuple");
            }
            BoolQueryBuilder tuple = (BoolQueryBuilder) alternative;
            List<Object> values = new ArrayList<>();
            for (QueryBuilder clause : tuple.filter()) {
                if ((clause instanceof TermQueryBuilder) == false) {
                    throw new AssertionError("Expected one native scalar equality");
                }
                TermQueryBuilder term = (TermQueryBuilder) clause;
                values.add(term.value());
            }
            assertTrue(rows.add(List.copyOf(values)));
        }
        return rows;
    }

    private static BytesRef key(List<DataType> types, Object... values) {
        // Fixture encoding has its own real breaker so ownership checks measure only the filter's independent copy.
        var encoding = breaker("1mb");
        try (
            var codec = new SequenceKeyCodec(types, IntStream.range(0, types.size()).toArray(), encoding);
            Page input = new Page(1, BlockUtils.fromArrayRow(TestBlockFactory.getNonBreakingInstance(), values));
            var key = codec.encode(input, 0)
        ) {
            return BytesRef.deepCopyOf(key.bytes());
        } finally {
            assertEquals(0, encoding.getUsed());
        }
    }

    private static CircuitBreaker breaker(String limit) {
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
