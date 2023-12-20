/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.hamcrest.Matchers.closeTo;

public class InternalCartesianBoundsTests extends InternalAggregationTestCase<InternalCartesianBounds> {
    static final double GEOHASH_TOLERANCE = 1E-5D;

    @Override
    protected SearchPlugin registerPlugin() {
        return new LocalStateSpatialPlugin();
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(
                Aggregation.class,
                new ParseField(CartesianBoundsAggregationBuilder.NAME),
                (p, c) -> ParsedCartesianBounds.fromXContent(p, (String) c)
            )
        );
    }

    @Override
    protected InternalCartesianBounds createTestInstance(String name, Map<String, Object> metadata) {
        // we occasionally want to test top = Double.NEGATIVE_INFINITY since this triggers empty xContent object
        double top = frequently() ? randomDouble() : Double.NEGATIVE_INFINITY;
        return new InternalCartesianBounds(name, top, randomDouble(), randomDouble(), randomDouble(), metadata);
    }

    @Override
    protected void assertReduced(InternalCartesianBounds reduced, List<InternalCartesianBounds> inputs) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double left = Double.POSITIVE_INFINITY;
        double right = Double.NEGATIVE_INFINITY;
        for (InternalCartesianBounds bounds : inputs) {
            top = max(top, bounds.top);
            bottom = min(bottom, bounds.bottom);
            left = min(left, bounds.left);
            right = max(right, bounds.right);
        }
        assertValueClose(reduced.top, top);
        assertValueClose(reduced.bottom, bottom);
        assertValueClose(reduced.left, left);
        assertValueClose(reduced.right, right);
    }

    private static void assertValueClose(double expected, double actual) {
        if (Double.isInfinite(expected) == false) {
            assertThat(expected, closeTo(actual, GEOHASH_TOLERANCE));
        } else {
            assertTrue(Double.isInfinite(actual));
        }
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalCartesianBounds sampled, InternalCartesianBounds reduced, SamplingContext samplingContext) {
        assertValueClose(sampled.top, reduced.top);
        assertValueClose(sampled.bottom, reduced.bottom);
        assertValueClose(sampled.left, reduced.left);
        assertValueClose(sampled.right, reduced.right);
    }

    @Override
    protected void assertFromXContent(InternalCartesianBounds aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedCartesianBounds);
        ParsedCartesianBounds parsed = (ParsedCartesianBounds) parsedAggregation;

        assertEquals(aggregation.topLeft(), parsed.topLeft());
        assertEquals(aggregation.bottomRight(), parsed.bottomRight());
    }

    @Override
    protected InternalCartesianBounds mutateInstance(InternalCartesianBounds instance) {
        String name = instance.getName();
        double top = instance.top;
        double bottom = instance.bottom;
        double left = instance.left;
        double right = instance.right;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 5)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                if (Double.isFinite(top)) {
                    top += between(1, 20);
                } else {
                    top = randomDouble();
                }
                break;
            case 2:
                bottom += between(1, 20);
                break;
            case 3:
                left += between(1, 20);
                break;
            case 4:
                right += between(1, 20);
                break;
            case 5:
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalCartesianBounds(name, top, bottom, left, right, metadata);
    }
}
