/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesBuilder;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.transforms.QueryConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

public class SchemaUtilTests extends ESTestCase {

    public void testInsertNestedObjectMappings() {
        Map<String, String> fieldMappings = new HashMap<>();
        // creates: a.b, a
        fieldMappings.put("a.b.c", "long");
        fieldMappings.put("a.b.d", "double");
        // creates: c.b, c
        fieldMappings.put("c.b.a", "double");
        // creates: c.d
        fieldMappings.put("c.d.e", "object");
        fieldMappings.put("d", "long");
        fieldMappings.put("e.f.g", "long");
        // cc: already there
        fieldMappings.put("e.f", "object");
        // cc: already there but different type (should not be possible)
        fieldMappings.put("e", "long");
        // cc: start with . (should not be possible)
        fieldMappings.put(".x", "long");
        // cc: start and ends with . (should not be possible), creates: .y
        fieldMappings.put(".y.", "long");
        // cc: ends with . (should not be possible), creates: .z
        fieldMappings.put(".z.", "long");

        SchemaUtil.insertNestedObjectMappings(fieldMappings);

        assertEquals(18, fieldMappings.size());
        assertEquals("long", fieldMappings.get("a.b.c"));
        assertEquals("object", fieldMappings.get("a.b"));
        assertEquals("double", fieldMappings.get("a.b.d"));
        assertEquals("object", fieldMappings.get("a"));
        assertEquals("object", fieldMappings.get("c.d"));
        assertEquals("object", fieldMappings.get("e.f"));
        assertEquals("long", fieldMappings.get("e"));
        assertEquals("object", fieldMappings.get(".y"));
        assertEquals("object", fieldMappings.get(".z"));
        assertFalse(fieldMappings.containsKey("."));
        assertFalse(fieldMappings.containsKey(""));
    }

    public void testConvertToIntegerTypeIfNeeded() {
        assertEquals(33L, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("unsigned_long", 33.0));
        assertEquals(33L, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("long", 33.0));
        assertEquals(33.0, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("double", 33.0));
        assertEquals(33.0, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("half_float", 33.0));
        assertEquals(33.0, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("unknown", 33.0));
        assertEquals(33.0, SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt(null, 33.0));

        Object value = SchemaUtil.dropFloatingPointComponentIfTypeRequiresIt("unsigned_long", 1.8446744073709551615E19);
        assertThat(value, instanceOf(BigInteger.class));

        assertEquals(new BigInteger("18446744073709551615").doubleValue(), ((BigInteger) value).doubleValue(), 0.0);
    }

    public void testGetSourceFieldMappings() throws InterruptedException {
        try (var threadPool = createThreadPool()) {
            final var client = new FieldCapsMockClient(threadPool, emptySet());
            // fields is null
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new SourceConfig(new String[] { "index-1", "index-2" }),
                    null,
                    listener
                ),
                mappings -> assertThat(mappings, anEmptyMap())
            );

            // fields is empty
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new SourceConfig(new String[] { "index-1", "index-2" }),
                    new String[] {},
                    listener
                ),
                mappings -> assertThat(mappings, anEmptyMap())
            );

            // good use
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new SourceConfig(new String[] { "index-1", "index-2" }),
                    new String[] { "field-1", "field-2" },
                    listener
                ),
                mappings -> assertThat(mappings, matchesMap(Map.of("field-1", "long", "field-2", "long")))
            );
        }
    }

    public void testGetSourceFieldMappingsWithRuntimeMappings() throws InterruptedException {
        Map<String, Object> runtimeMappings = Map.of("field-2", Map.of("type", "keyword"), "field-3", Map.of("type", "boolean"));
        try (var threadPool = createThreadPool()) {
            final var client = new FieldCapsMockClient(threadPool, emptySet());
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.getSourceFieldMappings(
                    client,
                    emptyMap(),
                    new SourceConfig(new String[] { "index-1", "index-2" }, QueryConfig.matchAll(), runtimeMappings),
                    new String[] { "field-1", "field-2" },
                    listener
                ),
                mappings -> {
                    assertThat(mappings, is(aMapWithSize(3)));
                    assertThat(
                        mappings,
                        allOf(hasEntry("field-1", "long"), hasEntry("field-2", "keyword"), hasEntry("field-3", "boolean"))
                    );
                }
            );
        }
    }

    public void testIsNumericType() {
        assertFalse(SchemaUtil.isNumericType(null));
        assertFalse(SchemaUtil.isNumericType("non-existing"));
        assertTrue(SchemaUtil.isNumericType("double"));
        assertTrue(SchemaUtil.isNumericType("integer"));
        assertTrue(SchemaUtil.isNumericType("long"));
        assertFalse(SchemaUtil.isNumericType("date"));
        assertFalse(SchemaUtil.isNumericType("date_nanos"));
        assertFalse(SchemaUtil.isNumericType("keyword"));
    }

    public void testIsDateType() {
        assertFalse(SchemaUtil.isDateType(null));
        assertFalse(SchemaUtil.isDateType("non-existing"));
        assertFalse(SchemaUtil.isDateType("double"));
        assertFalse(SchemaUtil.isDateType("integer"));
        assertFalse(SchemaUtil.isDateType("long"));
        assertTrue(SchemaUtil.isDateType("date"));
        assertTrue(SchemaUtil.isDateType("date_nanos"));
        assertFalse(SchemaUtil.isDateType("keyword"));
    }

    public void testDeduceMappings_AllMappingsArePresent() throws InterruptedException {
        testDeduceMappings(
            emptySet(),
            Map.of("by-day", "long", "by-user", "long", "by-business", "long", "timestamp", "long", "review_score", "double")
        );
    }

    public void testDeduceMappings_GroupByFieldMappingIsMissing() throws InterruptedException {
        testDeduceMappings(
            Set.of("business_id"),
            // Note that the expected mapping of the "by-business" target field is "keyword"
            Map.of("by-day", "long", "by-user", "long", "by-business", "keyword", "timestamp", "long", "review_score", "double")
        );
    }

    public void testDeduceMappings_AggregationFieldMappingIsMissing() throws InterruptedException {
        testDeduceMappings(
            Set.of("review_score"),
            Map.of("by-day", "long", "by-user", "long", "by-business", "long", "timestamp", "long", "review_score", "double")
        );
    }

    private void testDeduceMappings(Set<String> fieldsWithoutMappings, Map<String, String> expectedMappings) throws InterruptedException {
        try (var threadPool = createThreadPool()) {
            final var client = new FieldCapsMockClient(threadPool, fieldsWithoutMappings);
            var groups = Map.of(
                "by-day",
                new DateHistogramGroupSource(
                    "timestamp",
                    null,
                    false,
                    new DateHistogramGroupSource.CalendarInterval(DateHistogramInterval.DAY),
                    null,
                    null
                ),
                "by-user",
                new TermsGroupSource("user_id", null, false),
                "by-business",
                new TermsGroupSource("business_id", null, false)
            );
            var aggs = AggregatorFactories.builder()
                .addAggregator(AggregationBuilders.avg("review_score").field("stars"))
                .addAggregator(AggregationBuilders.max("timestamp").field("timestamp"));
            var groupConfig = new GroupConfig(emptyMap() /* unused anyway */, groups);
            var aggregationConfig = new AggregationConfig(emptyMap() /* unused anyway */, aggs);
            var pivotConfig = new PivotConfig(groupConfig, aggregationConfig, null);
            this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.deduceMappings(
                    client,
                    emptyMap(),
                    "my-transform",
                    new SettingsConfig.Builder().setDeduceMappings(randomBoolean() ? randomBoolean() : null).build(),
                    pivotConfig,
                    new SourceConfig(new String[] { "index-1", "index-2" }),
                    listener
                ),
                mappings -> assertThat(mappings, is(equalTo(expectedMappings)))
            );
        }
    }

    private static class FieldCapsMockClient extends NoOpClient {

        private final Set<String> fieldsWithoutMappings;

        FieldCapsMockClient(ThreadPool threadPool, Set<String> fieldsWithoutMappings) {
            super(threadPool);
            this.fieldsWithoutMappings = Objects.requireNonNull(fieldsWithoutMappings);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            if (request instanceof FieldCapabilitiesRequest fieldCapsRequest) {
                Map<String, Map<String, FieldCapabilities>> responseMap = new HashMap<>();
                for (String field : fieldCapsRequest.fields()) {
                    if (fieldsWithoutMappings.contains(field)) {
                        // If the field mappings should be missing, do **not** put it in the response.
                    } else {
                        responseMap.put(field, singletonMap(field, createFieldCapabilities(field, "long")));
                    }
                }
                for (Map.Entry<String, Object> runtimeField : fieldCapsRequest.runtimeFields().entrySet()) {
                    String field = runtimeField.getKey();
                    String type = (String) ((Map) runtimeField.getValue()).get("type");
                    responseMap.put(field, singletonMap(field, createFieldCapabilities(field, type)));
                }

                final FieldCapabilitiesResponse response = new FieldCapabilitiesResponse(fieldCapsRequest.indices(), responseMap);
                listener.onResponse((Response) response);
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    private static FieldCapabilities createFieldCapabilities(String name, String type) {
        return new FieldCapabilitiesBuilder(name, type).indices(Strings.EMPTY_ARRAY)
            .nonSearchableIndices(Strings.EMPTY_ARRAY)
            .nonAggregatableIndices(Strings.EMPTY_ARRAY)
            .build();
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

}
