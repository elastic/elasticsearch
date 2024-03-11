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

import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.BEHAVIORAL_ANALYTICS_TRANSPORT_VERSION;
import static org.elasticsearch.xpack.core.application.EnterpriseSearchFeatureSetUsage.QUERY_RULES_TRANSPORT_VERSION;

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
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected Writeable.Reader<EnterpriseSearchFeatureSetUsage> instanceReader() {
        return EnterpriseSearchFeatureSetUsage::new;
    }

    @Override
    protected EnterpriseSearchFeatureSetUsage mutateInstanceForVersion(EnterpriseSearchFeatureSetUsage instance, TransportVersion version) {
        if (version.onOrAfter(QUERY_RULES_TRANSPORT_VERSION)) {
            return instance;
        } else if (version.onOrAfter(BEHAVIORAL_ANALYTICS_TRANSPORT_VERSION)) {
            return new EnterpriseSearchFeatureSetUsage(
                true,
                true,
                instance.getSearchApplicationsUsage(),
                instance.getAnalyticsCollectionsUsage(),
                Map.of()
            );
        } else {
            return new EnterpriseSearchFeatureSetUsage(true, true, instance.getSearchApplicationsUsage(), Map.of(), Map.of());
        }
    }
}
