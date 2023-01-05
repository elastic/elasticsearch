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

package co.elastic.elasticsearch.stateless;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public abstract class AbstractStatelessIntegTestCase extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Stateless.class);
    }

    protected Settings.Builder nodeSettings() {
        return Settings.builder()
            .put(Stateless.STATELESS_ENABLED.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.FS)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), getTestName() + "_bucket");
    }

    protected String startIndexNode() {
        return internalCluster().startNode(
            nodeSettings().putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.INDEX_ROLE.roleName()).build()
        );
    }

    protected String startSearchNode() {
        return internalCluster().startNode(
            nodeSettings().putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName()).build()
        );
    }

    protected String startMasterOnlyNode() {
        return internalCluster().startMasterOnlyNode(nodeSettings().build());
    }

    protected String startMasterAndIndexNode() {
        return internalCluster().startNode(
            nodeSettings().putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                List.of(DiscoveryNodeRole.MASTER_ROLE.roleName(), DiscoveryNodeRole.INDEX_ROLE.roleName())
            ).build()
        );
    }

    protected List<String> startIndexNodes(int numOfNodes) {
        final List<String> nodes = new ArrayList<>(numOfNodes);
        for (int i = 0; i < numOfNodes; i++) {
            nodes.add(startIndexNode());
        }
        return List.copyOf(nodes);
    }

    protected List<String> startSearchNodes(int numOfNodes) {
        final List<String> nodes = new ArrayList<>(numOfNodes);
        for (int i = 0; i < numOfNodes; i++) {
            nodes.add(startSearchNode());
        }
        return List.copyOf(nodes);
    }

    protected static void indexDocs(String indexName, int numDocs) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        assertNoFailures(bulkRequest.get());
    }
}
