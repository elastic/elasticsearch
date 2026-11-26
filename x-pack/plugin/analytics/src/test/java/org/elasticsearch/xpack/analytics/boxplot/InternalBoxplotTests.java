/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.apache.lucene.tests.util.RamUsageTester;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.HistogramUnionState;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class InternalBoxplotTests extends InternalAggregationTestCase<InternalBoxplot> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin();
    }

    @Override
    protected InternalBoxplot createTestInstance(String name, Map<String, Object> metadata) {
        int numValues = frequently() ? randomInt(100) : 0;
        HistogramUnionState state = HistogramUnionState.create(HistogramUnionState.NOOP_BREAKER, 100);
        for (int i = 0; i < numValues; ++i) {
            state.add(randomDouble());
        }
        DocValueFormat formatter = randomNumericDocValueFormat();

        return new InternalBoxplot(name, state, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalBoxplot reduced, List<InternalBoxplot> inputs) {
        HistogramUnionState expected = HistogramUnionState.createUsingParamsFrom(reduced.state());
        for (InternalBoxplot input : inputs) {
            expected.add(input.state());
        }
        assertNotNull(expected);
        assertEquals(expected.getMax(), reduced.getMax(), 0);
        assertEquals(expected.getMin(), reduced.getMin(), 0);
    }

    @Override
    protected InternalBoxplot mutateInstance(InternalBoxplot instance) {
        String name = instance.getName();
        HistogramUnionState state;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            instance.state().writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                state = HistogramUnionState.read(HistogramUnionState.NOOP_BREAKER, in);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        DocValueFormat formatter = instance.format();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> state.add(randomDouble());
            case 2 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalBoxplot(name, state, formatter, metadata);
    }

    /**
     * A boxplot keeps a digest whose arrays are sized by its compression and not by the values it holds, so a coordinating node holding one
     * per bucket has to charge for its heap and not for its serialized form.
     */
    public void testEstimateHeapBytesCountsTheDigest() {
        assumeTrue("the object sizes assume compressed references", RamUsageEstimator.COMPRESSED_REFS_ENABLED);
        int buckets = 100;
        List<StringTerms.Bucket> list = new ArrayList<>(buckets);
        for (int i = 0; i < buckets; i++) {
            HistogramUnionState state = HistogramUnionState.create(HistogramUnionState.NOOP_BREAKER, 100);
            for (int v = 0; v < 2000; v++) {
                state.add(randomDouble() * 1000);
            }
            InternalAggregations sub = InternalAggregations.from(new InternalBoxplot("boxplot", state, DocValueFormat.RAW, null));
            BytesRef key = new BytesRef(String.format(Locale.ROOT, "%032d", i).getBytes(StandardCharsets.UTF_8));
            list.add(new StringTerms.Bucket(key, 1, sub, false, 0, DocValueFormat.RAW));
        }
        InternalAggregations aggregations = InternalAggregations.from(
            new StringTerms(
                "terms",
                BucketOrder.key(true),
                BucketOrder.count(false),
                buckets,
                1,
                null,
                DocValueFormat.RAW,
                buckets,
                false,
                0,
                list,
                0L
            )
        );
        long serialized = DelayableWriteable.getUncompressedSerializedSize(aggregations);
        long heap = RamUsageTester.ramUsed(aggregations);
        assertThat("the digests must be far larger than their serialized form", heap, greaterThan(5 * serialized));
        assertThat("estimate over heap", (double) aggregations.estimateHeapBytes() / heap, both(greaterThan(0.8)).and(lessThan(1.3)));
    }

    public void testIQR() {
        double epsilon = 0.00001; // tolerance on equality for doubles
        HistogramUnionState state = HistogramUnionState.create(HistogramUnionState.NOOP_BREAKER, 100);
        for (double value : List.of(52, 57, 57, 58, 63, 66, 66, 67, 67, 68, 69, 70, 70, 70, 70, 72, 73, 75, 75, 76, 76, 78, 79, 89)) {
            state.add(value);
        }
        double[] actual = InternalBoxplot.whiskers(state);
        assertEquals(57.0, actual[0], epsilon);
        assertEquals(79.0, actual[1], epsilon);

        // Test null state
        actual = InternalBoxplot.whiskers(null);
        assertNotNull(actual);
        assertTrue(Double.isNaN(actual[0]));
        assertTrue(Double.isNaN(actual[1]));
    }

    public void testIterator() {
        InternalBoxplot aggregation = createTestInstance("test", emptyMap());
        List<String> names = StreamSupport.stream(aggregation.valueNames().spliterator(), false).collect(Collectors.toList());

        assertEquals(7, names.size());
        assertTrue(names.contains("min"));
        assertTrue(names.contains("max"));
        assertTrue(names.contains("q1"));
        assertTrue(names.contains("q2"));
        assertTrue(names.contains("q3"));
        assertTrue(names.contains("lower"));
        assertTrue(names.contains("upper"));
    }
}
