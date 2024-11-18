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
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "_cat/tasks"));
            String tasks = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            assertThat(tasks, Matchers.containsString("health-node"));
        });
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "_health_report"));
            Map<String, Object> health_report = entityAsMap(response.getEntity());
            assertThat(health_report.get("status"), equalTo("green"));
        });
    }
}
