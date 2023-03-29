/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.dfs;

import org.apache.lucene.tests.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.profile.query.RandomQueryGenerator.randomQueryBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class DfsProfilerIT extends ESIntegTestCase {

    private static final int KNN_DIM = 3;

    public void testProfileDfs() throws Exception {
        String textField = "text_field";
        String numericField = "number";
        String vectorField = "vector";
        String indexName = "text-dfs-profile";
        createIndex(indexName, vectorField);
        ensureGreen();

        int numDocs = randomIntBetween(10, 50);
        IndexRequestBuilder[] docs = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            docs[i] = client().prepareIndex(indexName)
                .setId(String.valueOf(i))
                .setSource(
                    textField,
                    English.intToEnglish(i),
                    numericField,
                    i,
                    vectorField,
                    new float[] { randomFloat(), randomFloat(), randomFloat() }
                );
        }
        indexRandom(true, docs);
        refresh();
        int iters = between(5, 10);
        for (int i = 0; i < iters; i++) {
            QueryBuilder q = randomQueryBuilder(List.of(textField), List.of(numericField), numDocs, 3);
            logger.info("Query: {}", q);
            SearchResponse resp = client().prepareSearch()
                .setQuery(q)
                .setTrackTotalHits(true)
                .setProfile(true)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setKnnSearch(
                    randomList(
                        2,
                        5,
                        () -> new KnnSearchBuilder(
                            vectorField,
                            new float[] { randomFloat(), randomFloat(), randomFloat() },
                            randomIntBetween(5, 10),
                            50,
                            randomBoolean() ? null : randomFloat()
                        )
                    )
                )
                .get();

            assertNotNull("Profile response element should not be null", resp.getProfileResults());
            assertThat("Profile response should not be an empty array", resp.getProfileResults().size(), not(0));
            for (Map.Entry<String, SearchProfileShardResult> shard : resp.getProfileResults().entrySet()) {
                for (QueryProfileShardResult searchProfiles : shard.getValue().getQueryProfileResults()) {
                    for (ProfileResult result : searchProfiles.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                    }
                    CollectorResult result = searchProfiles.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                }
                SearchProfileDfsPhaseResult searchProfileDfsPhaseResult = shard.getValue().getSearchProfileDfsPhaseResult();
                assertThat(searchProfileDfsPhaseResult, is(notNullValue()));
                for (QueryProfileShardResult queryProfileShardResult : searchProfileDfsPhaseResult.getQueryProfileShardResult()) {
                    for (ProfileResult result : queryProfileShardResult.getQueryResults()) {
                        assertNotNull(result.getQueryName());
                        assertNotNull(result.getLuceneDescription());
                        assertThat(result.getTime(), greaterThan(0L));
                    }
                    CollectorResult result = queryProfileShardResult.getCollectorResult();
                    assertThat(result.getName(), is(not(emptyOrNullString())));
                    assertThat(result.getTime(), greaterThan(0L));
                    assertThat(result.getTime(), greaterThan(0L));
                }
                ProfileResult statsResult = searchProfileDfsPhaseResult.getDfsShardResult();
                assertThat(statsResult.getQueryName(), equalTo("statistics"));
            }
        }
    }

    private void createIndex(String name, String vectorField) throws IOException {
        assertAcked(
            prepareCreate(name).setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject(vectorField)
                    .field("type", "dense_vector")
                    .field("dims", KNN_DIM)
                    .field("index", true)
                    .field("similarity", "cosine")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
    }

}
