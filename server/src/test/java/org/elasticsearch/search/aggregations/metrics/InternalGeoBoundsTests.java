/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;

public class InternalGeoBoundsTests extends InternalAggregationTestCase<InternalGeoBounds> {
    static final double GEOHASH_TOLERANCE = 1E-5D;

    @Override
    protected InternalGeoBounds createTestInstance(String name, Map<String, Object> metadata) {
        // we occasionally want to test top = Double.NEGATIVE_INFINITY since this triggers empty xContent object
        double top = frequently() ? randomDouble() : Double.NEGATIVE_INFINITY;
        InternalGeoBounds geo = new InternalGeoBounds(
            name,
            top,
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomDouble(),
            randomBoolean(),
            metadata
        );
        return geo;
    }

    @Override
    protected void assertReduced(InternalGeoBounds reduced, List<InternalGeoBounds> inputs) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        for (InternalGeoBounds bounds : inputs) {
            if (bounds.top > top) {
                top = bounds.top;
            }
            if (bounds.bottom < bottom) {
                bottom = bounds.bottom;
            }
            if (bounds.posLeft < posLeft) {
                posLeft = bounds.posLeft;
            }
            if (bounds.posRight > posRight) {
                posRight = bounds.posRight;
            }
            if (bounds.negLeft < negLeft) {
                negLeft = bounds.negLeft;
            }
            if (bounds.negRight > negRight) {
                negRight = bounds.negRight;
            }
        }
        assertValueClose(reduced.top, top);
        assertValueClose(reduced.bottom, bottom);
        assertValueClose(reduced.posLeft, posLeft);
        assertValueClose(reduced.posRight, posRight);
        assertValueClose(reduced.negLeft, negLeft);
        assertValueClose(reduced.negRight, negRight);
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
    protected void assertSampled(InternalGeoBounds sampled, InternalGeoBounds reduced, SamplingContext samplingContext) {
        assertValueClose(sampled.top, reduced.top);
        assertValueClose(sampled.bottom, reduced.bottom);
        assertValueClose(sampled.posLeft, reduced.posLeft);
        assertValueClose(sampled.posRight, reduced.posRight);
        assertValueClose(sampled.negLeft, reduced.negLeft);
        assertValueClose(sampled.negRight, reduced.negRight);
    }

    @Override
    protected void assertFromXContent(InternalGeoBounds aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedGeoBounds);
        ParsedGeoBounds parsed = (ParsedGeoBounds) parsedAggregation;

        assertEquals(aggregation.topLeft(), parsed.topLeft());
        assertEquals(aggregation.bottomRight(), parsed.bottomRight());
    }

    @Override
    protected InternalGeoBounds mutateInstance(InternalGeoBounds instance) {
        String name = instance.getName();
        double top = instance.top;
        double bottom = instance.bottom;
        double posLeft = instance.posLeft;
        double posRight = instance.posRight;
        double negLeft = instance.negLeft;
        double negRight = instance.negRight;
        boolean wrapLongitude = instance.wrapLongitude;
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 8)) {
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
                posLeft += between(1, 20);
                break;
            case 4:
                posRight += between(1, 20);
                break;
            case 5:
                negLeft += between(1, 20);
                break;
            case 6:
                negRight += between(1, 20);
                break;
            case 7:
                wrapLongitude = wrapLongitude == false;
                break;
            case 8:
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
        return new InternalGeoBounds(name, top, bottom, posLeft, posRight, negLeft, negRight, wrapLongitude, metadata);
    }
}
