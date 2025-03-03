/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

public class ApmAgentSettingsIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("apm")
        .systemProperty("es.entitlements.enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testChangingApmAgentSettingsIsAllowed() throws Exception {
        var settings = Settings.builder().put("telemetry.metrics.enabled", true);
        APMAgentSettings.PERMITTED_AGENT_KEYS.stream().forEach(key -> settings.put("telemetry.agent." + key, "value"));
        updateClusterSettings(settings.build());

        updateClusterSettings(Settings.builder().put("telemetry.metrics.enabled", false).build());
    }
}
