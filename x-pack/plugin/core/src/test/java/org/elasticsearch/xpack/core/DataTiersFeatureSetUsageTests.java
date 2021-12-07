/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataTiersFeatureSetUsageTests extends AbstractWireSerializingTestCase<DataTiersFeatureSetUsage> {
    @Override
    protected Writeable.Reader<DataTiersFeatureSetUsage> instanceReader() {
        return DataTiersFeatureSetUsage::new;
    }

    @Override
    protected DataTiersFeatureSetUsage mutateInstance(DataTiersFeatureSetUsage instance) throws IOException {
        return randomValueOtherThan(instance, DataTiersFeatureSetUsageTests::randomUsage);
    }

    @Override
    protected DataTiersFeatureSetUsage createTestInstance() {
        return randomUsage();
    }

    public static DataTiersFeatureSetUsage randomUsage() {
        List<String> tiers = randomSubsetOf(DataTier.ALL_DATA_TIERS);
        Map<String, DataTiersFeatureSetUsage.TierSpecificStats> stats = new HashMap<>();
        tiers.forEach(
            tier -> stats.put(
                tier,
                new DataTiersFeatureSetUsage.TierSpecificStats(
                    randomIntBetween(1, 10),
                    randomIntBetween(5, 100),
                    randomIntBetween(0, 1000),
                    randomIntBetween(0, 1000),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                )
            )
        );
        return new DataTiersFeatureSetUsage(stats);
    }
}
