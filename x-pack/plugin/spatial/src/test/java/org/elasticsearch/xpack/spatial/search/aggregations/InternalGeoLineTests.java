/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class InternalGeoLineTests extends InternalAggregationTestCase<InternalGeoLine> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new SpatialPlugin();
    }

    @Override
    protected InternalGeoLine createTestInstance(String name, Map<String, Object> metadata) {
        int length = randomIntBetween(2, 2 * GeoLineAggregator.MAX_PATH_SIZE);
        long[] points = new long[length];
        double[] sortVals = new double[length];
        for (int i = 0; i < length; i++) {
            points[i] = i;
            sortVals[i] = i;
        }
        boolean complete = length <= GeoLineAggregator.MAX_PATH_SIZE;
        return new InternalGeoLine(name, points, sortVals, metadata, complete, randomBoolean(), randomFrom(SortOrder.values()));
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
        switch (randomIntBetween(0, 6)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                line[0] = line[0] + 1000000L;
                break;
            case 2:
                sortVals[0] = sortVals[0] + 10000;
                break;
            case 3:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
            case 4:
                complete = !complete;
                break;
            case 5:
                includeSorts = !includeSorts;
                break;
            case 6:
                sortOrder = SortOrder.ASC.equals(sortOrder) ? SortOrder.DESC : SortOrder.ASC;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalGeoLine(name, line, sortVals, metadata, complete, includeSorts, sortOrder);
    }

    @Override
    protected List<InternalGeoLine> randomResultsToReduce(String name, int size) {
        return Stream.generate(() -> createTestInstance(name, null)).limit(size).collect(toList());
    }

    @Override
    protected void assertReduced(InternalGeoLine reduced, List<InternalGeoLine> inputs) {
        // TODO(talevy)
        // assert final line is sorted
    }

    @Override
    protected void assertFromXContent(InternalGeoLine aggregation, ParsedAggregation parsedAggregation) throws IOException {
        // There is no ParsedGeoLine yet so we cannot test it here
    }

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<NamedXContentRegistry.Entry> extendedNamedXContents = new ArrayList<>(super.getNamedXContents());
        extendedNamedXContents.add(new NamedXContentRegistry.Entry(Aggregation.class,
            new ParseField(GeoLineAggregationBuilder.NAME),
            (p, c) -> {
                assumeTrue("There is no ParsedGeoLine yet", false);
                return null;
            }
        ));
        return extendedNamedXContents;
    }
}
