/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling;

import co.elastic.elasticsearch.serverless.autoscaling.ServerlessAutoscalingPlugin;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDesiredTopologyContextIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(ServerlessAutoscalingPlugin.class), super.nodePlugins());
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    public void testPostAutoscalingUpdatesDesiredTopologyContext() throws Exception {
        startMasterAndIndexNode(); // node 0
        startMasterAndIndexNode(); // node 1
        startSearchNode();
        ensureGreen();

        String content = """
            {
                "desired": {
                    "tier": {
                        "search": {
                            "replicas": 2,
                            "memory": "20G",
                            "storageRatio": 32.5,
                            "cpuRatio": 0.5,
                            "cpuLimitRatio": 1.5
                        },
                        "index": {
                            "replicas": 1,
                            "memory": "8G",
                            "storageRatio": 15.5,
                            "cpuRatio": 0.2,
                            "cpuLimitRatio": 1.0
                        }
                    }
                }
            }""";
        var postRequest = new Request("POST", "/_internal/serverless/autoscaling");
        postRequest.setJsonEntity(content);
        var desiredClusterTopology = DesiredClusterTopology.fromXContent(createParser(XContentType.JSON.xContent(), content));
        var desiredTopologyContext = internalCluster().getCurrentMasterNodeInstance(DesiredTopologyContext.class);
        var response = getRestClient().performRequest(postRequest);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertBusy(() -> assertThat(desiredClusterTopology, equalTo(desiredTopologyContext.getDesiredClusterTopology())));

        masterNodeAbdicatesForGracefulShutdown(); // Master: node 0 -> node 1
        var newDesiredTopologyContext = internalCluster().getCurrentMasterNodeInstance(DesiredTopologyContext.class);
        assertNotSame(desiredTopologyContext, newDesiredTopologyContext);
        assertNull(newDesiredTopologyContext.getDesiredClusterTopology());

        masterNodeAbdicatesForGracefulShutdown(); // Master: node 1 -> node 0
        var oldDesiredTopologyContext = internalCluster().getCurrentMasterNodeInstance(DesiredTopologyContext.class);
        assertSame(desiredTopologyContext, oldDesiredTopologyContext);
        assertNull(oldDesiredTopologyContext.getDesiredClusterTopology());
    }
}
