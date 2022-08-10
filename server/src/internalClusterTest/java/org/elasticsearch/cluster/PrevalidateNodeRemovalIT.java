/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.node.remove.NodesRemovalPrevalidation;
import org.elasticsearch.action.admin.cluster.node.remove.PrevalidateNodeRemovalAction;
import org.elasticsearch.action.admin.cluster.node.remove.PrevalidateNodeRemovalRequest;
import org.elasticsearch.action.admin.cluster.node.remove.PrevalidateNodeRemovalResponse;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class PrevalidateNodeRemovalIT extends ESIntegTestCase {

    public void testNodeRemovalFromGreenClusterIsSafe() throws Exception {
        createIndex("test");
        ensureGreen();
        String nodeName = randomFrom(internalCluster().getNodeNames());
        PrevalidateNodeRemovalRequest req = new PrevalidateNodeRemovalRequest(nodeName);
        PrevalidateNodeRemovalResponse resp = client().execute(PrevalidateNodeRemovalAction.INSTANCE, req).get();

        assertThat(resp.getPrevalidation().getOverallResult().isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
        assertThat(resp.getPrevalidation().getPerNodeResult().size(), equalTo(1));
        assertThat(resp.getPrevalidation().getPerNodeResult().containsKey(nodeName), equalTo(true));
        assertThat(resp.getPrevalidation().getPerNodeResult().get(nodeName).isSafe(), equalTo(NodesRemovalPrevalidation.IsSafe.YES));
    }
}
