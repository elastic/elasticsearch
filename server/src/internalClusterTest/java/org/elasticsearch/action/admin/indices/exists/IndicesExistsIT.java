/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.exists;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertRequestBuilderThrows;

@ClusterScope(
    scope = ESIntegTestCase.Scope.TEST,
    numDataNodes = 0,
    numClientNodes = 0,
    transportClientRatio = 0.0,
    autoManageMasterNodes = false
)
public class IndicesExistsIT extends ESIntegTestCase {

    public void testIndexExistsWithBlocksInPlace() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        Settings settings = Settings.builder().put(GatewayService.RECOVER_AFTER_NODES_SETTING.getKey(), 99).build();
        String node = internalCluster().startNode(settings);

        assertRequestBuilderThrows(
            client(node).admin().indices().prepareExists("test").setMasterNodeTimeout(TimeValue.timeValueSeconds(0)),
            MasterNotDiscoveredException.class
        );

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(node)); // shut down node so that test properly cleans up
    }
}
