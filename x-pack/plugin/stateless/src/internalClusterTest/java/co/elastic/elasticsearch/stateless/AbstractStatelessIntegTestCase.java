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

    protected String startIndexNode() {
        return internalCluster().startNode(
            Settings.builder()
                .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.INDEX_ROLE.roleName())
                .put(Stateless.STATELESS_ENABLED.getKey(), true)
                .build()
        );
    }

    protected String startSearchNode() {
        return internalCluster().startNode(
            Settings.builder()
                .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
                .put(Stateless.STATELESS_ENABLED.getKey(), true)
                .build()
        );
    }

    protected String startMasterOnlyNode() {
        return internalCluster().startMasterOnlyNode(Settings.builder().put(Stateless.STATELESS_ENABLED.getKey(), true).build());
    }

    protected String startMasterAndIndexNode() {
        return internalCluster().startNode(
            Settings.builder()
                .putList(
                    NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                    List.of(DiscoveryNodeRole.MASTER_ROLE.roleName(), DiscoveryNodeRole.INDEX_ROLE.roleName())
                )
                .put(Stateless.STATELESS_ENABLED.getKey(), true)
                .build()
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
