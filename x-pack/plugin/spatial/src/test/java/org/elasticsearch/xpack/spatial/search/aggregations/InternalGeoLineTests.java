/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class InternalGeoLineTests extends InternalAggregationTestCase<InternalGeoLine> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new SpatialPlugin();
    }

    static InternalGeoLine randomInstance(String name, Map<String, Object> metadata, int size, SortOrder sortOrder, double magicDecimal) {
        int length = randomIntBetween(2, size);
        long[] points = new long[length];
        double[] sortVals = new double[length];
        for (int i = 0; i < length; i++) {
            points[i] = randomNonNegativeLong();
            sortVals[i] = i + magicDecimal;
        }
        Arrays.sort(sortVals);
        if (SortOrder.DESC.equals(sortOrder)) {
            // reverse the list
            for (int i = 0, j = sortVals.length - 1; i < j; i++, j--) {
                double tmp = sortVals[i];
                sortVals[i] = sortVals[j];
                sortVals[j] = tmp;
            }
        }
        boolean complete = length <= size;
        return new InternalGeoLine(name, points, sortVals, metadata, complete, randomBoolean(), sortOrder, size);
    }

    @Override
    protected InternalGeoLine createTestInstance(String name, Map<String, Object> metadata) {
        int size = randomIntBetween(10, GeoLineAggregationBuilder.MAX_PATH_SIZE);
        return randomInstance(name, metadata, size, randomFrom(SortOrder.values()), randomDoubleBetween(0, 1, false));
    }

    @Override
    protected InternalGeoLine mutateInstance(InternalGeoLine instance) {
        String name = instance.getName();
        long[] line = Arrays.copyOf(instance.line(), instance.line().length);
        double[] sortVals = Arrays.copyOf(instance.sortVals(), instance.sortVals().length);
        Map<String, Object> metadata = instance.getMetadata();
        boolean complete = instance.isComplete();
        boolean includeSorts = instance.includeSorts();
        SortOrder sortOrder = instance.sortOrder();
        int size = instance.size();
        switch (randomIntBetween(0, 7)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> line[0] = line[0] + 1000000L;
            case 2 -> sortVals[0] = sortVals[0] + 10000;
            case 3 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            case 4 -> complete = complete == false;
            case 5 -> includeSorts = includeSorts == false;
            case 6 -> sortOrder = SortOrder.ASC.equals(sortOrder) ? SortOrder.DESC : SortOrder.ASC;
            case 7 -> size = size + 1;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalGeoLine(name, line, sortVals, metadata, complete, includeSorts, sortOrder, size);
    }

    @Override
    protected BuilderAndToReduce<InternalGeoLine> randomResultsToReduce(String name, int size) {
        SortOrder sortOrder = randomFrom(SortOrder.values());
        int maxLineLength = randomIntBetween(10, GeoLineAggregationBuilder.MAX_PATH_SIZE);
        List<InternalGeoLine> instances = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // use the magicDecimal to have absolute ordering between heap-sort and testing array sorting
            instances.add(randomInstance(name, null, maxLineLength, sortOrder, ((double) i) / size));
        }
        return new BuilderAndToReduce<>(mock(AggregationBuilder.class), instances);
    }

    @Override
    protected void assertReduced(InternalGeoLine reduced, List<InternalGeoLine> inputs) {
        int mergedLength = 0;
        for (InternalGeoLine subLine : inputs) {
            mergedLength += subLine.length();
        }
        boolean complete = mergedLength <= reduced.size();
        int expectedReducedLength = Math.min(mergedLength, reduced.size());
        assertThat(reduced.length(), equalTo(expectedReducedLength));
        assertThat(complete, equalTo(reduced.isComplete()));

        // check arrays
        long[] finalList = new long[mergedLength];
        double[] finalSortVals = new double[mergedLength];
        int idx = 0;
        for (InternalGeoLine geoLine : inputs) {
            for (int i = 0; i < geoLine.line().length; i++) {
                finalSortVals[idx] = geoLine.sortVals()[i];
                finalList[idx] = geoLine.line()[i];
                idx += 1;
            }
        }

        new PathArraySorter(finalList, finalSortVals, reduced.sortOrder()).sort();

        // cap to max length
        long[] finalCappedPoints = Arrays.copyOf(finalList, Math.min(reduced.size(), mergedLength));
        double[] finalCappedSortVals = Arrays.copyOf(finalSortVals, Math.min(reduced.size(), mergedLength));

        if (SortOrder.DESC.equals(reduced.sortOrder())) {
            new PathArraySorter(finalCappedPoints, finalCappedSortVals, SortOrder.ASC).sort();
        }

        assertArrayEquals(finalCappedSortVals, reduced.sortVals(), 0d);
        assertArrayEquals(finalCappedPoints, reduced.line());
    }

    @Override
    protected void assertFromXContent(InternalGeoLine aggregation, ParsedAggregation parsedAggregation) throws IOException {
        // There is no ParsedGeoLine yet so we cannot test it here
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        return CollectionUtils.appendToCopy(
            super.getNamedXContents(),
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(GeoLineAggregationBuilder.NAME), (p, c) -> {
                assumeTrue("There is no ParsedGeoLine yet", false);
                return null;
            })
        );
    }
}
