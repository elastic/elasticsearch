/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.eval;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.rank.vectors.LocalStateRankVectors;
import org.elasticsearch.xpack.rank.vectors.RankVectorsPlugin;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsReplicationIT;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0)
public class RankVectorsEvalIT extends ESIntegTestCase {

    private static final String INDEX = "rank-vectors-eval";
    private static final int DIMS = 128;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateRankVectors.class, RankVectorsReplicationIT.LicensedDiskBBQPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    public void testMultiShardExactCandidatesAndFinal() throws Exception {
        createIndex();
        index("best-0", 0, tokens(0, 1));
        index("best-1", 1, tokens(0, 1));
        index("other-0", 0, tokens(3, 4));
        index("other-1", 1, tokens(5, 6));
        refresh(INDEX);

        RankVectorsEvalRequest request = new RankVectorsEvalRequest(
            new RankVectorsEvalSpec("vectors", tokens(0, 1), 2, 2, true),
            new String[] { INDEX }
        );
        RankVectorsEvalResponse response = client().execute(RankVectorsPlugin.RANK_VECTORS_EVAL_ACTION, request).actionGet();
        assertEquals(2, response.primaryShards());
        assertEquals(2, response.baselineCount());
        assertTrue(response.candidateCount() >= 2);
        assertEquals(1.0, response.candidateRecall(), 0.0);
        assertEquals(1.0, response.finalRecall(), 0.0);
        assertEquals(Set.of("best-0", "best-1"), response.exactHits().stream().map(RankVectorsEvalResponse.Hit::id).collect(toSet()));
        assertEquals(Set.of("best-0", "best-1"), response.finalHits().stream().map(RankVectorsEvalResponse.Hit::id).collect(toSet()));
    }

    private void createIndex() throws Exception {
        try (
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("vectors")
                .field("type", "rank_vectors")
                .field("dims", DIMS)
                .field("index", true)
                .startObject("index_options")
                .field("type", "muvera")
                .field("encoding_version", 1)
                .field("encoding_profile", "muvera_v1")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        ) {
            assertAcked(indicesAdmin().prepareCreate(INDEX).setSettings(indexSettings(2, 0)).setMapping(mapping));
        }
        ensureGreen(INDEX);
    }

    private void index(String id, int shard, float[][] vectors) throws Exception {
        try (XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("vectors", vectors).endObject()) {
            prepareIndex(INDEX).setId(id).setRouting(routingKeyForShard(INDEX, shard)).setSource(source).get();
        }
    }

    private static float[][] tokens(int first, int second) {
        float[][] vectors = new float[2][DIMS];
        vectors[0][first] = 1f;
        vectors[1][second] = 1f;
        return vectors;
    }
}
