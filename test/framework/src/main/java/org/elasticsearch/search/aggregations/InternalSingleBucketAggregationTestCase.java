/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.metrics.InternalMin;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public abstract class InternalSingleBucketAggregationTestCase<T extends InternalSingleBucketAggregation>
        extends InternalAggregationTestCase<T> {

    private boolean hasInternalMax;
    private boolean hasInternalMin;

    public Supplier<InternalAggregations> subAggregationsSupplier;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hasInternalMax = randomBoolean();
        hasInternalMin = randomBoolean();
        subAggregationsSupplier = () -> {
            List<InternalAggregation> aggs = new ArrayList<>();
            if (hasInternalMax) {
                aggs.add(new InternalMax("max", randomDouble(), randomNumericDocValueFormat(), emptyMap()));
            }
            if (hasInternalMin) {
                aggs.add(new InternalMin("min", randomDouble(), randomNumericDocValueFormat(), emptyMap()));
            }
            return InternalAggregations.from(aggs);
        };
    }

    protected abstract T createTestInstance(String name, long docCount, InternalAggregations aggregations, Map<String, Object> metadata);
    protected abstract void extraAssertReduced(T reduced, List<T> inputs);

    @Override
    protected final T createTestInstance(String name, Map<String, Object> metadata) {
        // we shouldn't use the full long range here since we sum doc count on reduce, and don't want to overflow the long range there
        long docCount = between(0, Integer.MAX_VALUE);
        return createTestInstance(name, docCount, subAggregationsSupplier.get(), metadata);
    }

    @Override
    protected T mutateInstance(T instance) {
        String name = instance.getName();
        long docCount = instance.getDocCount();
        InternalAggregations aggregations = instance.getAggregations();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            docCount += between(1, 2000);
            break;
        case 2:
            List<InternalAggregation> aggs = new ArrayList<>();
            aggs.add(new InternalMax("new_max", randomDouble(), randomNumericDocValueFormat(), emptyMap()));
            aggs.add(new InternalMin("new_min", randomDouble(), randomNumericDocValueFormat(), emptyMap()));
            aggregations = InternalAggregations.from(aggs);
            break;
        case 3:
        default:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        }
        return createTestInstance(name, docCount, aggregations, metadata);
    }

    @Override
    protected final void assertReduced(T reduced, List<T> inputs) {
        assertEquals(inputs.stream().mapToLong(InternalSingleBucketAggregation::getDocCount).sum(), reduced.getDocCount());
        if (hasInternalMax) {
            double expected = inputs.stream().mapToDouble(i -> {
                        InternalMax max = i.getAggregations().get("max");
                        return max.getValue();
                    }).max().getAsDouble();
            InternalMax reducedMax = reduced.getAggregations().get("max");
            assertEquals(expected, reducedMax.getValue(), 0);
        }
        if (hasInternalMin) {
            double expected = inputs.stream().mapToDouble(i -> {
                        InternalMin min = i.getAggregations().get("min");
                        return min.getValue();
                    }).min().getAsDouble();
            InternalMin reducedMin = reduced.getAggregations().get("min");
            assertEquals(expected, reducedMin.getValue(), 0);
        }
        extraAssertReduced(reduced, inputs);
    }

    @Override
    protected void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) throws IOException {
        assertTrue(parsedAggregation instanceof ParsedSingleBucketAggregation);
        ParsedSingleBucketAggregation parsed = (ParsedSingleBucketAggregation) parsedAggregation;

        assertEquals(aggregation.getDocCount(), parsed.getDocCount());
        InternalAggregations aggregations = aggregation.getAggregations();
        Map<String, Aggregation> expectedAggregations = new HashMap<>();
        int expectedNumberOfAggregations = 0;
        for (Aggregation expectedAggregation : aggregations) {
            // since we shuffle xContent, we cannot rely on the order of the original inner aggregations for comparison
            assertTrue(expectedAggregation instanceof InternalAggregation);
            expectedAggregations.put(expectedAggregation.getName(), expectedAggregation);
            expectedNumberOfAggregations++;
        }
        int parsedNumberOfAggregations = 0;
        for (Aggregation parsedAgg : parsed.getAggregations()) {
            assertTrue(parsedAgg instanceof ParsedAggregation);
            assertTrue(expectedAggregations.keySet().contains(parsedAgg.getName()));
            Aggregation expectedInternalAggregation = expectedAggregations.get(parsedAgg.getName());
            final XContentType xContentType = randomFrom(XContentType.values());
            final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
            BytesReference expectedBytes = toXContent(expectedInternalAggregation, xContentType, params, false);
            BytesReference actualBytes = toXContent(parsedAgg, xContentType, params, false);
            assertToXContentEquivalent(expectedBytes, actualBytes, xContentType);
            parsedNumberOfAggregations++;
        }
        assertEquals(expectedNumberOfAggregations, parsedNumberOfAggregations);
        Class<? extends ParsedSingleBucketAggregation> parsedClass = implementationClass();
        assertTrue(parsedClass != null && parsedClass.isInstance(parsedAggregation));
    }

    protected abstract Class<? extends ParsedSingleBucketAggregation> implementationClass();
}
