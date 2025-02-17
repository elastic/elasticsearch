/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.hamcrest.Matchers;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class HealthNodeUpgradeIT extends AbstractRollingUpgradeTestCase {

    public HealthNodeUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testHealthNode() throws Exception {
        // Assert that the response from _cat/tasks returns health-node provided the health.supports_health feature is present
        if (clusterHasFeature("health.supports_health")) {
            assertBusy(() -> {
                Response response = client().performRequest(new Request("GET", "_cat/tasks"));
                String tasks = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                assertThat(tasks, Matchers.containsString("health-node"));
            });
        }

        // Assert that _health_report endpoint returns a green status provided the health.supports_health_report_api feature is present
        // N.B. If this feature is not present, the cluster may have a mixture of nodes which serve the health endpoint at /_health_report
        // and older nodes which serve it at /_internal/_health. There is no sensible and reliable way to test the endpoint in this case.
        if (clusterHasFeature("health.supports_health_report_api")) {
            assertBusy(() -> {
                Response response = client().performRequest(new Request("GET", "_health_report"));
                Map<String, Object> health_report = entityAsMap(response.getEntity());
                assertThat(health_report.get("status"), equalTo("green"));
            });
        }
    }
}
