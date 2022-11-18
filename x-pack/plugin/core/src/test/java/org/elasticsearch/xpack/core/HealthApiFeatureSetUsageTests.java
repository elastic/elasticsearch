/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.cluster.coordination.StableMasterHealthIndicatorService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.metrics.Counters;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.DiskHealthIndicatorService;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class HealthApiFeatureSetUsageTests extends AbstractWireSerializingTestCase<HealthApiFeatureSetUsage> {

    @Override
    protected HealthApiFeatureSetUsage createTestInstance() {
        return new HealthApiFeatureSetUsage(
            true,
            true,
            randomCounters(),
            Set.of(randomFrom(HealthStatus.values())),
            Map.of(HealthStatus.RED, Set.of(DiskHealthIndicatorService.NAME, StableMasterHealthIndicatorService.NAME)),
            Map.of()
        );
    }

    @Override
    protected HealthApiFeatureSetUsage mutateInstance(HealthApiFeatureSetUsage instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected Writeable.Reader<HealthApiFeatureSetUsage> instanceReader() {
        return HealthApiFeatureSetUsage::new;
    }

    private Counters randomCounters() {
        Counters counters = new Counters();
        for (int i = 0; i < randomInt(20); i++) {
            if (randomBoolean()) {
                counters.inc(randomAlphaOfLength(10), randomInt(20));
            } else {
                counters.inc(randomAlphaOfLength(10) + "." + randomAlphaOfLength(10), randomInt(20));
            }
        }
        return counters;
    }
}
