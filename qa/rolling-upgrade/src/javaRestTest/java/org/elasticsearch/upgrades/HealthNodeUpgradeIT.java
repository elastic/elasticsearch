/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        if (clusterHasFeature("health.supports_health")) {
            assertBusy(() -> {
                Response response = client().performRequest(new Request("GET", "_cat/tasks"));
                String tasks = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                assertThat(tasks, Matchers.containsString("health-node"));
            });
            assertBusy(() -> {
                String path = clusterHasFeature("health.supports_health_report_api") ? "_health_report" : "_internal/_health";
                Response response = client().performRequest(new Request("GET", path));
                Map<String, Object> health_report = entityAsMap(response.getEntity());
                assertThat(health_report.get("status"), equalTo("green"));
            });
        }
    }
}
