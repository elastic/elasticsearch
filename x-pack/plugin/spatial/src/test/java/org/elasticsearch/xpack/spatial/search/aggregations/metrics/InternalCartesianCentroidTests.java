/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalCentroid;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class InternalCartesianCentroidTests extends InternalAggregationTestCase<InternalCartesianCentroid> {

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
                new ParseField(CartesianCentroidAggregationBuilder.NAME),
                (p, c) -> ParsedCartesianCentroid.fromXContent(p, (String) c)
            )
        );
    }

    @Override
    protected InternalCartesianCentroid createTestInstance(String name, Map<String, Object> metadata) {
        Point point = ShapeTestUtils.randomPoint(false);
        CartesianPoint centroid = new CartesianPoint(point.getX(), point.getY());
        // Unlike InternalGeoCentroid, we do not need to encode/decode to handle hashcode test failures,
        // but we do need to treat zero values with care. See the mutate function below for details on that
        long count = randomIntBetween(0, 1000);
        if (count == 0) {
            centroid = null;
        }
        return new InternalCartesianCentroid(name, centroid, count, Collections.emptyMap());
    }

    @Override
    protected void assertReduced(InternalCartesianCentroid reduced, List<InternalCartesianCentroid> inputs) {
        double xSum = 0;
        double ySum = 0;
        long totalCount = 0;
        for (InternalCartesianCentroid input : inputs) {
            if (input.count() > 0) {
                xSum += (input.count() * input.centroid().getX());
                ySum += (input.count() * input.centroid().getY());
            }
            totalCount += input.count();
        }
        if (totalCount > 0) {
            assertThat(ySum / totalCount, closeTo(reduced.centroid().getY(), Math.abs(reduced.centroid().getY() / 1e10)));
            assertThat(xSum / totalCount, closeTo(reduced.centroid().getX(), Math.abs(reduced.centroid().getX() / 1e10)));
        }
        assertEquals(totalCount, reduced.count());
    }

    @Override
    protected boolean supportsSampling() {
        return true;
    }

    @Override
    protected void assertSampled(InternalCartesianCentroid sampled, InternalCartesianCentroid reduced, SamplingContext samplingContext) {
        assertThat(sampled.centroid().getY(), closeTo(reduced.centroid().getY(), Math.abs(reduced.centroid().getY() / 1e10)));
        assertThat(sampled.centroid().getX(), closeTo(reduced.centroid().getX(), Math.abs(reduced.centroid().getX() / 1e10)));
        assertEquals(sampled.count(), samplingContext.scaleUp(reduced.count()), 0);
    }

    public void testReduceMaxCount() {
        InternalCartesianCentroid maxValueCentroid = new InternalCartesianCentroid(
            "agg",
            new CartesianPoint(10, 0),
            Long.MAX_VALUE,
            Collections.emptyMap()
        );
        InternalCentroid reducedCentroid = maxValueCentroid.reduce(Collections.singletonList(maxValueCentroid), null);
        assertThat(reducedCentroid.count(), equalTo(Long.MAX_VALUE));
    }

    @Override
    protected void assertFromXContent(InternalCartesianCentroid aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedCartesianCentroid);
        ParsedCartesianCentroid parsed = (ParsedCartesianCentroid) parsedAggregation;

        assertEquals(aggregation.centroid(), parsed.centroid());
        assertEquals(aggregation.count(), parsed.count());
    }

    @Override
    protected InternalCartesianCentroid mutateInstance(InternalCartesianCentroid instance) {
        double minValue = -1000000;
        double maxValue = 1000000;
        String name = instance.getName();
        SpatialPoint centroid = instance.centroid();
        long count = instance.count();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                count += between(1, 100);
                if (centroid == null) {
                    // if the new count is > 0 then we need to make sure there is a
                    // centroid or the constructor will throw an exception
                    centroid = new CartesianPoint(
                        randomDoubleBetween(minValue, maxValue, false),
                        randomDoubleBetween(minValue, maxValue, false)
                    );
                }
            }
            case 2 -> {
                if (centroid == null) {
                    centroid = new CartesianPoint(
                        randomDoubleBetween(minValue, maxValue, false),
                        randomDoubleBetween(minValue, maxValue, false)
                    );
                    count = between(1, 100);
                } else {
                    CartesianPoint newCentroid = new CartesianPoint(centroid);
                    if (randomBoolean()) {
                        mutateCoordinate(centroid::getY, newCentroid::resetY);
                    } else {
                        mutateCoordinate(centroid::getX, newCentroid::resetX);
                    }
                    centroid = newCentroid;
                }
            }
            case 3 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalCartesianCentroid(name, centroid, count, metadata);
    }

    /**
     * The previous mutation of dividing by 2.0 left zero values unchanged, leading to lack of mutation.
     * Now we act differently on small values to ensure that mutation actually occurs.
     */
    private void mutateCoordinate(Supplier<Double> getter, Consumer<Double> setter) {
        double coordinate = getter.get();
        if (Math.abs(coordinate) < 1e-6) {
            setter.accept(coordinate + 1.0);
        } else {
            setter.accept(coordinate / 2.0);
        }
    }
}
