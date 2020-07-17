/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalTTestTests extends InternalAggregationTestCase<InternalTTest> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new AnalyticsPlugin();
    }

    @Override
    protected InternalTTest createTestInstance(String name, Map<String, Object> metadata) {
        TTestState state = randomState(Long.MAX_VALUE, randomFrom(TTestType.values()), randomIntBetween(1, 2));
        DocValueFormat formatter = randomNumericDocValueFormat();
        return new InternalTTest(name, state, formatter, metadata);
    }

    @Override
    protected List<InternalTTest> randomResultsToReduce(String name, int size) {
        TTestType type = randomFrom(TTestType.values());
        int tails = randomIntBetween(1, 2);
        List<InternalTTest> inputs = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // Make sure the sum of all the counts doesn't wrap and type and tail parameters are consistent
            TTestState state = randomState(Long.MAX_VALUE / size, type, tails);
            DocValueFormat formatter = randomNumericDocValueFormat();
            inputs.add(new InternalTTest(name, state, formatter, null));
        }
        return inputs;
    }

    private TTestState randomState(long maxCount, TTestType type, int tails) {
        if (type == TTestType.PAIRED) {
            return new PairedTTestState(randomStats(maxCount), tails);
        } else {
            return new UnpairedTTestState(randomStats(maxCount), randomStats(maxCount), type == TTestType.HOMOSCEDASTIC, tails);
        }
    }

    private TTestStats randomStats(long maxCount) {
        return new TTestStats(randomLongBetween(0, maxCount), randomDouble(), randomDouble());
    }

    @Override
    protected void assertReduced(InternalTTest reduced, List<InternalTTest> inputs) {
        TTestState expected = reduced.state.reduce(inputs.stream().map(a -> a.state));
        assertNotNull(expected);
        assertEquals(expected.getValue(), reduced.getValue(), 0.00001);
    }

    @Override
    protected void assertFromXContent(InternalTTest min, ParsedAggregation parsedAggregation) {
        // There is no ParsedTTest yet so we cannot test it here
    }

    @Override
    protected InternalTTest mutateInstance(InternalTTest instance) {
        String name = instance.getName();
        TTestState state;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeNamedWriteable(instance.state);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                state = in.readNamedWriteable(TTestState.class);
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
                state = randomState(Long.MAX_VALUE, randomFrom(TTestType.values()), randomIntBetween(1, 2));
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
        return new InternalTTest(name, state, formatter, metadata);
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> extendedNamedXContents = new ArrayList<>(super.getNamedXContents());
        extendedNamedXContents.add(new NamedXContentRegistry.Entry(Aggregation.class,
            new ParseField(TTestAggregationBuilder.NAME),
            (p, c) -> {
                assumeTrue("There is no ParsedTTest yet", false);
                return null;
            }
        ));
        return extendedNamedXContents;
    }
}
