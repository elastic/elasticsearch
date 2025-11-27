/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.node.SimpleHealthInfo;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;

import java.util.Map;

public class TestTracker extends HealthTracker<SimpleHealthInfo> {
    @Override
    protected SimpleHealthInfo determineCurrentHealth() {
        SimpleHealthInfo simpleHealthInfo = new SimpleHealthInfo(
            HealthStatus.YELLOW,
            "test symptom",
            Map.of("ID Number", String.valueOf(Math.random()))
        );
        return simpleHealthInfo;
    }

    @Override
    protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, SimpleHealthInfo healthInfo) {
        builder.simpleHealthInfoByTrackerName("test-tracker", healthInfo);
    }
}
