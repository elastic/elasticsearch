/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.query;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.rank.vectors.LocalStateRankVectors;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsReplicationIT;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

@LuceneTestCase.SuppressCodecs("*")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0)
public class RankVectorsSearchIT extends ESIntegTestCase {

    private static final String INDEX = "rank-vectors-search";
    private static final String FIELD = "vectors";
    private static final int DIMS = 128;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateRankVectors.class, RankVectorsReplicationIT.LicensedDiskBBQPlugin.class);
    }

    public void testQueryAndRetrieverAcrossShards() throws Exception {
        createIndex();
        indexDocument("exact", routingKeyForShard(INDEX, 0), "en", unitTokens(0, 1));
        indexDocument("partial", routingKeyForShard(INDEX, 1), "en", unitTokens(0, 2));
        indexDocument("filtered-out", routingKeyForShard(INDEX, 1), "fr", unitTokens(0, 1));
        indexDocument("unrelated", routingKeyForShard(INDEX, 0), "en", unitTokens(2, 3));
        refresh(INDEX);

        float[][] queryVectors = unitTokens(0, 1);
        RankVectorsQueryBuilder query = new RankVectorsQueryBuilder("vectors_alias", queryVectors, 2, 4).addFilterQuery(
            QueryBuilders.termQuery("language", "en")
        );
        assertResponse(
            client().prepareSearch(INDEX).setQuery(query).setSize(2),
            response -> assertExpectedHits(response.getHits().getHits())
        );

        RankVectorsRetrieverBuilder retriever = new RankVectorsRetrieverBuilder(FIELD, queryVectors, 2, 4).addFilterQuery(
            QueryBuilders.termQuery("language", "en")
        );
        SearchSourceBuilder source = new SearchSourceBuilder().retriever(retriever).size(2);
        assertResponse(client().prepareSearch(INDEX).setSource(source), response -> assertExpectedHits(response.getHits().getHits()));
    }

    private void createIndex() throws Exception {
        try (
            XContentBuilder mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(FIELD)
                .field("type", "rank_vectors")
                .field("dims", DIMS)
                .field("index", true)
                .startObject("index_options")
                .field("type", "muvera")
                .field("encoding_version", 1)
                .field("encoding_profile", "muvera_v1")
                .endObject()
                .endObject()
                .startObject("vectors_alias")
                .field("type", "alias")
                .field("path", FIELD)
                .endObject()
                .startObject("language")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
        ) {
            assertAcked(indicesAdmin().prepareCreate(INDEX).setSettings(indexSettings(2, 0)).setMapping(mapping));
        }
        ensureGreen(INDEX);
    }

    private void indexDocument(String id, String routing, String language, float[][] vectors) throws Exception {
        try (
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("language", language)
                .field(FIELD, vectors)
                .endObject()
        ) {
            prepareIndex(INDEX).setId(id).setRouting(routing).setSource(source).get();
        }
    }

    private static void assertExpectedHits(SearchHit[] hits) {
        assertEquals(2, hits.length);
        assertEquals("exact", hits[0].getId());
        assertEquals(3f, hits[0].getScore(), 0f);
        assertEquals("partial", hits[1].getId());
        assertEquals(2f, hits[1].getScore(), 0f);
    }

    private static float[][] unitTokens(int firstDimension, int secondDimension) {
        float[][] vectors = new float[2][DIMS];
        vectors[0][firstDimension] = 1f;
        vectors[1][secondDimension] = 1f;
        return vectors;
    }
}
