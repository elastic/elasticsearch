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
import org.elasticsearch.blobcache.BlobCachePlugin;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public abstract class AbstractStatelessIntegTestCase extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(BlobCachePlugin.class, Stateless.class);
    }

    private boolean useBasePath;

    @Before
    public void setup() {
        useBasePath = randomBoolean();
    }

    protected String getFsRepoSanitizedBucketName() {
        return getTestName().replaceAll("[^0-9a-zA-Z-_]", "_") + "_bucket";
    }

    protected Settings.Builder nodeSettings() {
        final Settings.Builder builder = Settings.builder()
            .put(Stateless.STATELESS_ENABLED.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.FS)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), getFsRepoSanitizedBucketName());
        if (useBasePath) {
            builder.put(ObjectStoreService.BASE_PATH_SETTING.getKey(), "base_path");
        }
        return builder;
    }

    protected String startIndexNode() {
        return internalCluster().startNode(
            nodeSettings().putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.INDEX_ROLE.roleName()).build()
        );
    }

    protected String startSearchNode() {
        return internalCluster().startNode(
            nodeSettings().putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
                .put(
                    SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                    rarely()
                        ? randomBoolean()
                            ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.KB).getStringRep()
                            : new ByteSizeValue(randomIntBetween(1, 1000), ByteSizeUnit.BYTES).getStringRep()
                        : randomBoolean() ? new ByteSizeValue(randomIntBetween(1, 10), ByteSizeUnit.MB).getStringRep()
                        // only use up to 0.1% disk to be friendly.
                        : new RatioValue(randomDoubleBetween(0.0d, 0.1d, false)).toString()
                )
                .build()
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

    protected void indexDocsAndRefresh(String indexName, int numDocs) throws Exception {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(new IndexRequest(indexName).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        boolean bulkRefreshes = randomBoolean();
        if (bulkRefreshes) {
            bulkRequest.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL));
        }
        assertNoFailures(bulkRequest.get());
        if (bulkRefreshes == false) {
            assertNoFailures(client().admin().indices().prepareRefresh(indexName).execute().get());
        }
    }

    @Override
    protected boolean addMockFSIndexStore() {
        return false;
    }
}
