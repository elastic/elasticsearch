/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class UpdateSettingsValidationIT extends ESIntegTestCase {
    public void testUpdateSettingsValidation() throws Exception {
        internalCluster().startNodes(nonDataNode(), nonMasterNode(), nonMasterNode());

        createIndex("test");
        NumShards test = getNumShards("test");

        ClusterHealthResponse healthResponse = clusterAdmin().prepareHealth("test")
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .setWaitForGreenStatus()
            .get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getIndices().get("test").getActiveShards(), equalTo(test.totalNumShards));

        setReplicaCount(0, "test");
        healthResponse = clusterAdmin().prepareHealth("test").setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        assertThat(healthResponse.getIndices().get("test").getActiveShards(), equalTo(test.numPrimaries));

        try {
            indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.refresh_interval", "")).get();
            fail();
        } catch (IllegalArgumentException ex) {
            logger.info("Error message: [{}]", ex.getMessage());
        }
    }
}
