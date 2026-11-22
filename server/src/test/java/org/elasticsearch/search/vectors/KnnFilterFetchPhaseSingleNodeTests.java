/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomFloatVector;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Checks when the filter of a knn search runs again in the fetch phase.
 * <p>
 * A knn query runs its whole search, filter included, while it is rewritten. When fetch is its own round (more than one shard),
 * it starts from a new search context, so fetch steps that need the query (scores for field-sorted hits with
 * {@code track_scores}, and {@code explain}) rewrite it again and the knn search and its filter run a second time. Named
 * queries are rewritten from scratch even on a single shard. A top-level {@code knn} section does not run again: after the DFS
 * round the query and fetch rounds only get the matching doc IDs and scores.
 * <p>
 * The filter is a script query that counts its calls. Script queries are never cached, so every evaluation is counted. Each
 * test compares two requests that differ only in the option under test.
 */
public class KnnFilterFetchPhaseSingleNodeTests extends ESSingleNodeTestCase {

    private static final int VECTOR_DIMENSION = 10;
    private static final int NUM_DOCS = 50;
    private static final String COUNTING_FILTER = "counting_filter";
    private static final AtomicInteger FILTER_CALLS = new AtomicInteger();

    public static class CountingFilterPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Map.of(COUNTING_FILTER, vars -> {
                FILTER_CALLS.incrementAndGet();
                return true;
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(CountingFilterPlugin.class);
    }

    public void testFetchScoresRunKnnFilterAgain() throws IOException {
        createIndexWithDocs(2);
        float[] queryVector = randomFloatVector(VECTOR_DIMENSION);

        int sortOnly = countFilterCalls(sortedSearch(knnQuery(queryVector)));
        int sortWithScores = countFilterCalls(sortedSearch(knnQuery(queryVector)).setTrackScores(true));

        assertThat(sortOnly, greaterThan(0));
        assertThat(sortWithScores, greaterThan(sortOnly));
    }

    public void testExplainRunsKnnFilterAgain() throws IOException {
        createIndexWithDocs(2);
        float[] queryVector = randomFloatVector(VECTOR_DIMENSION);

        int plain = countFilterCalls(client().prepareSearch("index").setQuery(knnQuery(queryVector)));
        int explained = countFilterCalls(client().prepareSearch("index").setQuery(knnQuery(queryVector)).setExplain(true));

        assertThat(plain, greaterThan(0));
        assertThat(explained, greaterThan(plain));
    }

    public void testNamedKnnQueryRunsFilterAgainOnOneShard() throws IOException {
        createIndexWithDocs(1);
        float[] queryVector = randomFloatVector(VECTOR_DIMENSION);

        int unnamed = countFilterCalls(client().prepareSearch("index").setQuery(knnQuery(queryVector)));
        int named = countFilterCalls(client().prepareSearch("index").setQuery(knnQuery(queryVector).queryName("knn")));

        assertThat(unnamed, greaterThan(0));
        assertThat(named, greaterThan(unnamed));
    }

    public void testFetchScoresReuseRewriteOnOneShard() throws IOException {
        createIndexWithDocs(1);
        float[] queryVector = randomFloatVector(VECTOR_DIMENSION);

        int sortOnly = countFilterCalls(sortedSearch(knnQuery(queryVector)));
        int sortWithScores = countFilterCalls(sortedSearch(knnQuery(queryVector)).setTrackScores(true));

        assertThat(sortOnly, greaterThan(0));
        assertThat(sortWithScores, equalTo(sortOnly));
    }

    public void testFetchScoresDoNotRunTopLevelKnnFilterAgain() throws IOException {
        createIndexWithDocs(2);
        float[] queryVector = randomFloatVector(VECTOR_DIMENSION);

        int sortOnly = countFilterCalls(sortedSearch(null).setKnnSearch(List.of(knnSection(queryVector))));
        int sortWithScores = countFilterCalls(sortedSearch(null).setKnnSearch(List.of(knnSection(queryVector))).setTrackScores(true));

        assertThat(sortOnly, greaterThan(0));
        assertThat(sortWithScores, equalTo(sortOnly));
    }

    private void createIndexWithDocs(int numShards) throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("vector")
            .field("type", "dense_vector")
            .field("dims", VECTOR_DIMENSION)
            .field("index", true)
            .field("similarity", "l2_norm")
            .endObject()
            .startObject("num")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();
        createIndex("index", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build(), mapping);
        for (int doc = 0; doc < NUM_DOCS; doc++) {
            prepareIndex("index").setId(Integer.toString(doc)).setSource("vector", randomFloatVector(VECTOR_DIMENSION), "num", doc).get();
        }
        indicesAdmin().prepareRefresh("index").get();
    }

    private static QueryBuilder countingFilter() {
        return QueryBuilders.scriptQuery(new Script(ScriptType.INLINE, MockScriptPlugin.NAME, COUNTING_FILTER, Map.of()));
    }

    private static KnnVectorQueryBuilder knnQuery(float[] queryVector) {
        return new KnnVectorQueryBuilder("vector", queryVector, 10, 50, null, null, null).addFilterQuery(countingFilter());
    }

    private static KnnSearchBuilder knnSection(float[] queryVector) {
        return new KnnSearchBuilder("vector", queryVector, 10, 50, null, null, null).addFilterQuery(countingFilter());
    }

    private SearchRequestBuilder sortedSearch(QueryBuilder query) {
        SearchRequestBuilder search = client().prepareSearch("index").addSort("num", SortOrder.ASC);
        return query == null ? search : search.setQuery(query);
    }

    /** Runs the search and returns how many times the knn filter was evaluated across all phases. */
    private static int countFilterCalls(SearchRequestBuilder search) {
        FILTER_CALLS.set(0);
        assertResponse(search, response -> assertThat(response.getHits().getHits().length, greaterThan(0)));
        return FILTER_CALLS.get();
    }
}
