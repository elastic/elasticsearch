package org.elasticsearch.node.service;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.hamcrest.Matchers.hasSize;

public class NodeServiceTests extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(NetworkModule.HTTP_ENABLED.getKey(), false).build();
    }

    public void testHttpServerDisabled() {
        // test for a bug where if HTTP stats were requested but HTTP was disabled, NodeService would hit a NullPointerException
        NodesStatsResponse response = client().admin().cluster().nodesStats(new NodesStatsRequest().http(true)).actionGet();
        assertThat(response.getNodes(), hasSize(1));
    }

}
