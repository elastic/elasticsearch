/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.upgrades.ParameterizedRollingUpgradeTestCase;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationPoller;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED;
import static org.hamcrest.Matchers.is;

public class AuthorizationTaskExecutorUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    private static final String BEFORE_AUTHORIZATION_TASK_FEATURE = "gte_v9.2.0";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterVersion(), isOldClusterDetachedVersion())
        .nodes(NODE_NUM)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false")
        // We need a url set for the authorization task to be created, but we don't actually care if we get a valid response
        // just that the task will be created upon upgrade
        .setting(ELASTIC_INFERENCE_SERVICE_URL.getKey(), "http://localhost:12345")
        .build();

    private static final String GET_METHOD = "GET";

    public AuthorizationTaskExecutorUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testUpgradeAuthorizationTaskExecutor() throws Exception {
        assumeTrue("Authorization Polling Task supported", oldClusterHasFeature(BEFORE_AUTHORIZATION_TASK_FEATURE));

        // If the old cluster already has the feature for the authorization polling task, the task should already exist
        // We only want to test when upgrading from a version that does not have the task
        if (oldClusterHasFeature(BEFORE_AUTHORIZATION_TASK_FEATURE) == false) {
            if (isOldCluster()) {
                // if we're on a version prior to the authorization polling task, the task should not be created
                assertFalse(doesAuthPollingTaskExist());
            }
            if (isMixedCluster()) {
                // if we're in the middle of an upgrade where some nodes are upgraded and some are not, the task should
                // still not be created. It should wait until all nodes are upgraded
                assertFalse(doesAuthPollingTaskExist());
            }
        }

        if (isUpgradedCluster()) {
            // once fully upgraded, the authorization polling task should be created
            assertBusy(() -> assertTrue(doesAuthPollingTaskExist()));
        }
    }

    @SuppressWarnings("unchecked")
    private static boolean doesAuthPollingTaskExist() throws IOException {
        var request = new Request(GET_METHOD, Strings.format("_tasks?pretty&actions=%s*", AuthorizationPoller.TASK_NAME));
        var response = adminClient().performRequest(request);
        assertStatusOkOrCreated(response);

        /*
        The task response will look like this
        {
            "nodes": {
                "jFlV8lS0SKip7Tp6Iz9Eew": {
                    ...
                    "tasks": {
                        "jFlV8lS0SKip7Tp6Iz9Eew:336": {
                            "node": "jFlV8lS0SKip7Tp6Iz9Eew",
                            "id": 336,
                            "type": "persistent",
                            "action": "eis-authorization-poller[c]",
                            ...
                        }
                    }
                }
            }
        }
         */
        var responseAsMap = entityAsMap(response);
        var nodes = (Map<String, Object>) responseAsMap.get("nodes");

        if (nodes == null || nodes.isEmpty()) {
            return false;
        }

        // There should only ever be a single authorization task in the cluster
        assertThat(nodes.size(), is(1));

        var node = (Map<String, Object>) nodes.values().iterator().next();
        var tasks = (Map<String, Object>) node.get("tasks");

        if (tasks == null || tasks.isEmpty()) {
            return false;
        }

        for (var taskObj : tasks.values()) {
            var task = (Map<String, Object>) taskObj;
            var action = (String) task.get("action");
            if (action != null && action.startsWith(AuthorizationPoller.TASK_NAME)) {
                return true;
            }
        }

        return false;
    }
}
