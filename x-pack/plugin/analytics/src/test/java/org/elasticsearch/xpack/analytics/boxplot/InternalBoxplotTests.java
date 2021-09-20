/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;

public class InternalBoxplotTests extends InternalAggregationTestCase<InternalBoxplot> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin(Settings.EMPTY);
    }

    @Override
    protected InternalBoxplot createTestInstance(String name, Map<String, Object> metadata) {
        int numValues = frequently() ? randomInt(100) : 0;
        TDigestState state = new TDigestState(100);
        for (int i = 0; i < numValues; ++i) {
            state.add(randomDouble());
        }
        DocValueFormat formatter = randomNumericDocValueFormat();

        return new InternalBoxplot(name, state, formatter, metadata);
    }

    @Override
    protected void assertReduced(InternalBoxplot reduced, List<InternalBoxplot> inputs) {
        TDigestState expected = new TDigestState(reduced.state().compression());
        for (InternalBoxplot input : inputs) {
            expected.add(input.state());
        }
        assertNotNull(expected);
        assertEquals(expected.getMax(), reduced.getMax(), 0);
        assertEquals(expected.getMin(), reduced.getMin(), 0);
    }

    @Override
    protected void assertFromXContent(InternalBoxplot min, ParsedAggregation parsedAggregation) {
        // There is no ParsedBoxplot yet so we cannot test it here
    }

    @Override
    protected InternalBoxplot mutateInstance(InternalBoxplot instance) {
        String name = instance.getName();
        TDigestState state;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            TDigestState.write(instance.state(), output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                state = TDigestState.read(in);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
        DocValueFormat formatter = instance.format();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                state.add(randomDouble());
                break;
            case 2:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalBoxplot(name, state, formatter, metadata);
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(BoxplotAggregationBuilder.NAME), (p, c) -> {
                assumeTrue("There is no ParsedBoxlot yet", false);
                return null;
            })
        );
    }

    public void testIQR() {
        double epsilon = 0.00001; // tolerance on equality for doubles
        TDigestState state = new TDigestState(100);
        for (double value : org.elasticsearch.core.List.of(
            52,
            57,
            57,
            58,
            63,
            66,
            66,
            67,
            67,
            68,
            69,
            70,
            70,
            70,
            70,
            72,
            73,
            75,
            75,
            76,
            76,
            78,
            79,
            89
        )) {
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
