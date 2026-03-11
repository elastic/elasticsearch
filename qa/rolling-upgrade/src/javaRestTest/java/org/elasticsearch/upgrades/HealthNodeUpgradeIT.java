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

import org.elasticsearch.client.Request;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;

/**
 * Rolling upgrade test that verifies the health node task is running and assigned after a cluster upgrade.
 */
public class HealthNodeUpgradeIT extends AbstractRollingUpgradeTestCase {

    public HealthNodeUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testHealthNodeTaskRunningAfterUpgrade() throws Exception {
        if (isUpgradedCluster()) {
            assertHealthNodeTaskAssigned();
            assertHealthReportAvailable();
        }
    }

    private void assertHealthNodeTaskAssigned() throws IOException {
        final var response = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_cluster/state/metadata")));
        final List<?> tasks = response.evaluate("metadata.persistent_tasks.tasks");
        assertThat("persistent tasks must exist", tasks, notNullValue());
        String executorNode = null;
        for (int i = 0; i < tasks.size(); i++) {
            String taskId = response.evaluate("metadata.persistent_tasks.tasks." + i + ".id");
            if ("health-node".equals(taskId)) {
                executorNode = response.evaluate("metadata.persistent_tasks.tasks." + i + ".assignment.executor_node");
                break;
            }
        }
        assertThat("health node task must be assigned", executorNode, notNullValue());
    }

    private void assertHealthReportAvailable() throws IOException {
        final var healthReport = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_health_report")));
        final var status = healthReport.evaluate("status");
        assertThat("unexpected health report status", status, oneOf("green", "yellow", "red", "unknown"));
    }
}
