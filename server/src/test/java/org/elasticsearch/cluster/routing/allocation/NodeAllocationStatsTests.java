/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class NodeAllocationStatsTests extends AbstractWireSerializingTestCase<NodeAllocationStats> {

    @Override
    protected Writeable.Reader<NodeAllocationStats> instanceReader() {
        return NodeAllocationStats::new;
    }

    @Override
    protected NodeAllocationStats createTestInstance() {
        return randomNodeAllocationStats();
    }

    @Override
    protected NodeAllocationStats mutateInstance(NodeAllocationStats instance) throws IOException {
        return switch (randomInt(4)) {
            case 0 -> new NodeAllocationStats(
                randomValueOtherThan(instance.shards(), () -> randomIntBetween(0, 10000)),
                instance.undesiredShards(),
                instance.forecastedIngestLoad(),
                instance.forecastedDiskUsage(),
                instance.currentDiskUsage()
            );
            case 1 -> new NodeAllocationStats(
                instance.shards(),
                randomValueOtherThan(instance.undesiredShards(), () -> randomIntBetween(0, 1000)),
                instance.forecastedIngestLoad(),
                instance.forecastedDiskUsage(),
                instance.currentDiskUsage()
            );
            case 2 -> new NodeAllocationStats(
                instance.shards(),
                instance.undesiredShards(),
                randomValueOtherThan(instance.forecastedIngestLoad(), () -> randomDoubleBetween(0, 8, true)),
                instance.forecastedDiskUsage(),
                instance.currentDiskUsage()
            );
            case 3 -> new NodeAllocationStats(
                instance.shards(),
                instance.undesiredShards(),
                instance.forecastedIngestLoad(),
                randomValueOtherThan(instance.forecastedDiskUsage(), ESTestCase::randomNonNegativeLong),
                instance.currentDiskUsage()
            );
            case 4 -> new NodeAllocationStats(
                instance.shards(),
                instance.undesiredShards(),
                instance.forecastedIngestLoad(),
                instance.currentDiskUsage(),
                randomValueOtherThan(instance.forecastedDiskUsage(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new RuntimeException("unreachable");
        };
    }

    public static NodeAllocationStats randomNodeAllocationStats() {
        return new NodeAllocationStats(
            randomIntBetween(0, 10000),
            randomIntBetween(0, 1000),
            randomDoubleBetween(0, 8, true),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }
}
