/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.diskbbq.DiskBBQPlugin;
import org.elasticsearch.xpack.rank.vectors.LocalStateRankVectors;
import org.elasticsearch.xpack.rank.vectors.fde.MuveraEncoder;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 3, numClientNodes = 0)
public class RankVectorsReplicationIT extends ESIntegTestCase {

    private static final String INDEX = "rank-vectors-replication";
    private static final String FIELD = "vectors";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateRankVectors.class, LicensedDiskBBQPlugin.class);
    }

    public void testDerivedFdeIsIdenticalOnPrimaryAndReplicaAndAfterPromotion() throws Exception {
        assertAcked(
            indicesAdmin().prepareCreate(INDEX)
                .setSettings(indexSettings(1, 1))
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(FIELD)
                        .field("type", "rank_vectors")
                        .field("dims", RankVectorsFieldMapper.MUVERA_TOKEN_DIMS)
                        .field("index", true)
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );
        ensureGreen(INDEX);

        float[][] sourceVectors = unitTokens();
        try (XContentBuilder source = XContentFactory.jsonBuilder().startObject().field(FIELD, sourceVectors).endObject()) {
            prepareIndex(INDEX).setId("doc").setSource(source).get();
        }
        indicesAdmin().prepareRefresh(INDEX).get();
        ensureGreen(INDEX);

        float[] expected = encoder().encodeDocument(sourceVectors);
        Set<String> shardNodes = internalCluster().nodesInclude(INDEX);
        assertEquals(2, shardNodes.size());
        for (String node : shardNodes) {
            assertArrayEquals(expected, readFde(node), 0f);
        }

        String primaryNodeId = clusterService().state().routingTable().index(INDEX).shard(0).primaryShard().currentNodeId();
        String primaryNode = clusterService().state().nodes().get(primaryNodeId).getName();
        internalCluster().stopNode(primaryNode);
        ensureYellowAndNoInitializingShards(INDEX);

        String promotedNodeId = clusterService().state().routingTable().index(INDEX).shard(0).primaryShard().currentNodeId();
        String promotedNode = clusterService().state().nodes().get(promotedNodeId).getName();
        assertArrayEquals(expected, readFde(promotedNode), 0f);
    }

    private float[] readFde(String node) throws Exception {
        Index index = resolveIndex(INDEX);
        IndexShard shard = internalCluster().getInstance(IndicesService.class, node).indexServiceSafe(index).getShard(0);
        try (Engine.Searcher searcher = shard.acquireSearcher("verify replicated MUVERA FDE")) {
            assertEquals(1, searcher.getDirectoryReader().leaves().size());
            FloatVectorValues values = searcher.getDirectoryReader().leaves().getFirst().reader().getFloatVectorValues(FIELD);
            assertNotNull(values);
            assertEquals(RankVectorsFieldMapper.MUVERA_FDE_DIMS, values.dimension());
            assertEquals(1, values.size());
            KnnVectorValues.DocIndexIterator iterator = values.iterator();
            assertEquals(0, iterator.nextDoc());
            return Arrays.copyOf(values.vectorValue(iterator.index()), values.dimension());
        }
    }

    private static MuveraEncoder encoder() {
        return new MuveraEncoder(
            RankVectorsFieldMapper.MUVERA_TOKEN_DIMS,
            RankVectorsFieldMapper.MUVERA_K_SIM,
            RankVectorsFieldMapper.MUVERA_D_PROJ,
            RankVectorsFieldMapper.MUVERA_R_REPS,
            RankVectorsFieldMapper.MUVERA_SEED
        );
    }

    private static float[][] unitTokens() {
        float[][] vectors = new float[2][RankVectorsFieldMapper.MUVERA_TOKEN_DIMS];
        vectors[0][0] = 1f;
        vectors[1][1] = 1f;
        return vectors;
    }

    public static class LicensedDiskBBQPlugin extends DiskBBQPlugin {
        private final XPackLicenseState licenseState = new XPackLicenseState(
            System::currentTimeMillis,
            new XPackLicenseStatus(License.OperationMode.TRIAL, true, null)
        );

        public LicensedDiskBBQPlugin(Settings settings) {
            // DiskBBQ is licensed on stateless nodes. Pass that mode to the provider without changing the test nodes' engine mode.
            super(Settings.builder().put(settings).put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build());
        }

        @Override
        protected XPackLicenseState getLicenseState() {
            return licenseState;
        }
    }
}
