/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.application;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;
import java.util.Map;

public class EnterpriseSearchFeatureSetUsageBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    EnterpriseSearchFeatureSetUsage> {

    @Override
    protected EnterpriseSearchFeatureSetUsage createTestInstance() {
        Map<String, Object> searchApplicationsStats = Map.of(EnterpriseSearchFeatureSetUsage.COUNT, randomLongBetween(0, 100000));
        Map<String, Object> analyticsCollectionsStats = Map.of(EnterpriseSearchFeatureSetUsage.COUNT, randomLongBetween(0, 100000));
        Map<String, Object> queryRulesStats = Map.of(EnterpriseSearchFeatureSetUsage.COUNT, randomLongBetween(0, 100000));
        return new EnterpriseSearchFeatureSetUsage(true, true, searchApplicationsStats, analyticsCollectionsStats, queryRulesStats);
    }

    @Override
    protected EnterpriseSearchFeatureSetUsage mutateInstance(EnterpriseSearchFeatureSetUsage instance) throws IOException {
        Map<String, Object> searchApplicationsUsage = instance.getSearchApplicationsUsage();
        Map<String, Object> analyticsCollectionsUsage = instance.getAnalyticsCollectionsUsage();
        Map<String, Object> queryRulesUsage = instance.getQueryRulesUsage();
        switch (between(0, 2)) {
            case 0 -> searchApplicationsUsage = randomValueOtherThan(
                searchApplicationsUsage,
                () -> Map.of(EnterpriseSearchFeatureSetUsage.COUNT, randomLong())
            );
            case 1 -> analyticsCollectionsUsage = randomValueOtherThan(
                analyticsCollectionsUsage,
                () -> Map.of(EnterpriseSearchFeatureSetUsage.COUNT, randomLong())
            );
            case 2 -> queryRulesUsage = randomValueOtherThan(
                queryRulesUsage,
                () -> Map.of(EnterpriseSearchFeatureSetUsage.COUNT, randomLong())
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new EnterpriseSearchFeatureSetUsage(true, true, searchApplicationsUsage, analyticsCollectionsUsage, queryRulesUsage);
    }

    @Override
    protected Writeable.Reader<EnterpriseSearchFeatureSetUsage> instanceReader() {
        return EnterpriseSearchFeatureSetUsage::new;
    }

    @Override
    protected EnterpriseSearchFeatureSetUsage mutateInstanceForVersion(EnterpriseSearchFeatureSetUsage instance, TransportVersion version) {
        return instance;
    }
}
