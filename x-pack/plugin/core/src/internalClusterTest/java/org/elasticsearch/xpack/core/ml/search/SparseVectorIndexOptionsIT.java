/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.HttpStatus;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class SparseVectorIndexOptionsIT extends ESIntegTestCase {
    private static final String TEST_INDEX_NAME = "index_with_sparse_vector";
    private static final String SPARSE_VECTOR_FIELD = "sparse_vector_field";
    private static final int TEST_PRUNING_TOKENS_FREQ_THRESHOLD = 1;
    private static final float TEST_PRUNING_TOKENS_WEIGHT_THRESHOLD = 1.0f;

    private final boolean testHasIndexOptions;
    private final boolean testIndexShouldPrune;
    private final boolean testQueryShouldNotPrune;
    private final boolean overrideQueryPruningConfig;

    public SparseVectorIndexOptionsIT(boolean setIndexOptions, boolean setIndexShouldPrune, boolean setQueryShouldNotPrune) {
        this.testHasIndexOptions = setIndexOptions;
        this.testIndexShouldPrune = setIndexShouldPrune;
        this.testQueryShouldNotPrune = setQueryShouldNotPrune;
        this.overrideQueryPruningConfig = (testHasIndexOptions && testIndexShouldPrune) && randomBoolean();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        List<Object[]> params = new ArrayList<>();
        // create a matrix of all combinations
        // of our three parameters
        for (int i = 0; i < 8; i++) {
            params.add(new Object[] { (i & 1) == 0, (i & 2) == 0, (i & 4) == 0 });
        }
        return params;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(XPackClientPlugin.class);
    }

    @Before
    public void setup() {
        assertAcked(prepareCreate(TEST_INDEX_NAME).setMapping(getTestIndexMapping()));
        ensureGreen(TEST_INDEX_NAME);

        for (Map.Entry<String, String> doc : TEST_DOCUMENTS.entrySet()) {
            index(TEST_INDEX_NAME, doc.getKey(), doc.getValue());
        }
        flushAndRefresh(TEST_INDEX_NAME);
    }

    public void testSparseVectorTokenPruning() throws IOException {
        Response response = performSearch(getBuilderForSearch());
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        final Map<String, Object> responseMap = XContentHelper.convertToMap(
            XContentType.JSON.xContent(),
            response.getEntity().getContent(),
            true
        );
        assertCorrectResponse(responseMap);
    }

    @SuppressWarnings("unchecked")
    private void assertCorrectResponse(Map<String, Object> responseMap) {
        List<String> expectedIds = getTestExpectedDocIds();

        Map<String, Object> mapHits = (Map<String, Object>) responseMap.get("hits");
        Map<String, Object> mapHitsTotal = (Map<String, Object>) mapHits.get("total");
        int actualTotalHits = (int) mapHitsTotal.get("value");
        int numHitsExpected = expectedIds.size();

        assertEquals(getAssertMessage("Search result total hits count mismatch"), numHitsExpected, actualTotalHits);

        List<Map<String, Object>> hits = (List<Map<String, Object>>) mapHits.get("hits");
        List<String> actualDocIds = new ArrayList<>();
        for (Map<String, Object> doc : hits) {
            actualDocIds.add((String) doc.get("_id"));
        }

        assertEquals(getAssertMessage("Result document ids mismatch"), expectedIds, actualDocIds);
    }

    private String getTestIndexMapping() {
        String testPruningConfigMapping = "\"pruning_config\":{\"tokens_freq_ratio_threshold\":"
            + TEST_PRUNING_TOKENS_FREQ_THRESHOLD
            + ",\"tokens_weight_threshold\":"
            + TEST_PRUNING_TOKENS_WEIGHT_THRESHOLD
            + "}";

        String pruningMappingString = testIndexShouldPrune ? "\"prune\":true," + testPruningConfigMapping : "\"prune\":false";
        String indexOptionsString = testHasIndexOptions ? ",\"index_options\":{" + pruningMappingString + "}" : "";

        return "{\"properties\":{\"" + SPARSE_VECTOR_FIELD + "\":{\"type\":\"sparse_vector\"" + indexOptionsString + "}}}";
    }

    private List<String> getTestExpectedDocIds() {
        if (overrideQueryPruningConfig) {
            return EXPECTED_DOC_IDS_WITH_QUERY_OVERRIDE;
        }

        if (testQueryShouldNotPrune) {
            // query overrides prune = false in all cases
            return EXPECTED_DOC_IDS_WITHOUT_PRUNING;
        }

        if (testHasIndexOptions) {
            // index has set index options in the mapping
            return testIndexShouldPrune ? EXPECTED_DOC_IDS_WITH_PRUNING : EXPECTED_DOC_IDS_WITHOUT_PRUNING;
        }

        // default pruning should be true with default configuration
        return EXPECTED_DOC_IDS_WITH_DEFAULT_PRUNING;
    }

    private Response performSearch(String source) throws IOException {
        Request request = new Request("GET", TEST_INDEX_NAME + "/_search");
        request.setJsonEntity(source);
        return getRestClient().performRequest(request);
    }

    private String getBuilderForSearch() {
        boolean shouldUseDefaultTokens = (testQueryShouldNotPrune == false && testHasIndexOptions == false);
        TokenPruningConfig queryPruningConfig = overrideQueryPruningConfig
                                                ? new TokenPruningConfig(3f, 0.5f, true)
                                                : null;

        SparseVectorQueryBuilder queryBuilder =  new SparseVectorQueryBuilder(
            SPARSE_VECTOR_FIELD,
            shouldUseDefaultTokens ? SEARCH_WEIGHTED_TOKENS_WITH_DEFAULTS : SEARCH_WEIGHTED_TOKENS,
            null,
            null,
            overrideQueryPruningConfig ? Boolean.TRUE : (testQueryShouldNotPrune ? false : null),
            queryPruningConfig
        );

        return "{\"query\":" + Strings.toString(queryBuilder) + "}";
    }

    private String getAssertMessage(String message) {
        return message
            + " (Params: hasIndexOptions="
            + testHasIndexOptions
            + ", indexShouldPrune="
            + testIndexShouldPrune
            + ", queryShouldNotPrune="
            + testQueryShouldNotPrune
            + "): "
            + getDescriptiveTestType();
    }

    private String getDescriptiveTestType() {
        String testDescription = "";
        if (testQueryShouldNotPrune) {
            testDescription = "query override prune=false:";
        }

        if (testHasIndexOptions) {
            testDescription += " pruning index_options explicitly set:";
        } else {
            testDescription = " no index options set, tokens should be pruned by default:";
        }

        if (testIndexShouldPrune == false) {
            testDescription += " index options has pruning set to false";
        }

        return testDescription;
    }

    private static final Map<String, String> TEST_DOCUMENTS = Map.of(
        "1",
        "{\"sparse_vector_field\":{\"cheese\": 2.671405,\"is\": 0.11809908,\"comet\": 0.26088917}}",
        "2",
        "{\"sparse_vector_field\":{\"planet\": 2.3438394,\"is\": 0.54600334,\"astronomy\": 0.36015007,\"moon\": 0.20022368}}",
        "3",
        "{\"sparse_vector_field\":{\"is\": 0.6891394,\"globe\": 0.484035,\"ocean\": 0.080102935,\"underground\": 0.053516876}}"
    );

    private static final List<WeightedToken> SEARCH_WEIGHTED_TOKENS = List.of(
        new WeightedToken("cheese", 0.5f),
        new WeightedToken("comet", 0.5f),
        new WeightedToken("globe", 0.484035f),
        new WeightedToken("ocean", 0.080102935f),
        new WeightedToken("underground", 0.053516876f),
        new WeightedToken("is", 0.54600334f)
    );

    private static final List<WeightedToken> SEARCH_WEIGHTED_TOKENS_WITH_DEFAULTS = List.of(new WeightedToken("planet", 0.2f));

    private static final List<String> EXPECTED_DOC_IDS_WITHOUT_PRUNING = List.of("1", "3", "2");

    private static final List<String> EXPECTED_DOC_IDS_WITH_PRUNING = List.of();

    private static final List<String> EXPECTED_DOC_IDS_WITH_DEFAULT_PRUNING = List.of("2");

    private static final List<String> EXPECTED_DOC_IDS_WITH_QUERY_OVERRIDE = List.of();
}
