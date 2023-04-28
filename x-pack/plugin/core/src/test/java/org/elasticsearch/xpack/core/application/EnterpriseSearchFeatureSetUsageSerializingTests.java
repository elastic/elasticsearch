/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.application;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnterpriseSearchFeatureSetUsageSerializingTests extends AbstractWireSerializingTestCase<EnterpriseSearchFeatureSetUsage> {

    @Override
    protected EnterpriseSearchFeatureSetUsage createTestInstance() {
        Map<String, Object> searchApplicationsStats = new HashMap<>();
        searchApplicationsStats.put("count", randomLongBetween(0, 100000));
        return new EnterpriseSearchFeatureSetUsage(true, true, searchApplicationsStats);
    }

    @Override
    protected EnterpriseSearchFeatureSetUsage mutateInstance(EnterpriseSearchFeatureSetUsage instance) throws IOException {
        long searchApplicationsCount = (long) instance.getSearchApplicationsStats().get("count");
        searchApplicationsCount = randomValueOtherThan(searchApplicationsCount, () -> randomLongBetween(0, 100000));

        Map<String, Object> searchApplicationsStats = new HashMap<>();
        searchApplicationsStats.put("count", searchApplicationsCount);

        return new EnterpriseSearchFeatureSetUsage(true, true, searchApplicationsStats);
    }

    @Override
    protected Writeable.Reader<EnterpriseSearchFeatureSetUsage> instanceReader() {
        return EnterpriseSearchFeatureSetUsage::new;
    }
}
