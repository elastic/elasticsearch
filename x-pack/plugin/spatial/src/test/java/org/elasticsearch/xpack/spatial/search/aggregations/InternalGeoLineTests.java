/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.xpack.spatial.SpatialPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalGeoLineTests extends InternalAggregationTestCase<InternalGeoLine> {

    @Override
    protected SearchPlugin registerPlugin() {
        return new SpatialPlugin();
    }

    @Override
    protected InternalGeoLine createTestInstance(String name, Map<String, Object> metadata) {
        return null;
    }

    @Override
    protected InternalGeoLine mutateInstance(InternalGeoLine instance) {
        return null;
    }

    @Override
    protected List<InternalGeoLine> randomResultsToReduce(String name, int size) {
        return Collections.emptyList();
    }

    @Override
    protected void assertReduced(InternalGeoLine reduced, List<InternalGeoLine> inputs) {
    }

    @Override
    protected void assertFromXContent(InternalGeoLine aggregation, ParsedAggregation parsedAggregation) throws IOException {
        // There is no ParsedGeoLine yet so we cannot test it here
    }
}
