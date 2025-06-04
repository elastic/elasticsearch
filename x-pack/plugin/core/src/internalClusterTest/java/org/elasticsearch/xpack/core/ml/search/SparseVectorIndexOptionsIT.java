/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.search;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.vectors.TokenPruningConfig;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
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

    public void testSparseVectorTokenPruning() throws Exception {
        assertBusy(() -> {
            SearchResponse response = performSearch(getBuilderForSearch());
            try {
                assertThat(response.status(), Matchers.equalTo(RestStatus.OK));
                assertCorrectResponse(response);
            } finally {
                response.decRef();
            }
        });
    }

    private void assertCorrectResponse(SearchResponse response) {
        List<String> expectedIds = getTestExpectedDocIds();

        assertEquals(
            getAssertMessage("Search result total hits count mismatch"),
            expectedIds.size(),
            response.getHits().getTotalHits().value()
        );

        List<String> actualDocIds = new ArrayList<>();
        for (SearchHit doc : response.getHits().getHits()) {
            actualDocIds.add(doc.getId());
        }

        assertEquals(getAssertMessage("Result document ids mismatch"), expectedIds, actualDocIds);
    }

    private String getTestIndexMapping() {
        try {
            XContentBuilder docBuilder = XContentFactory.jsonBuilder();
            docBuilder.startObject();
            {
                docBuilder.startObject("properties");
                {
                    docBuilder.startObject(SPARSE_VECTOR_FIELD);
                    {
                        docBuilder.field("type", "sparse_vector");
                        addIndexFieldIndexOptions(docBuilder);
                    }
                    docBuilder.endObject();
                }
                docBuilder.endObject();
            }
            docBuilder.endObject();
            return Strings.toString(docBuilder);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void addIndexFieldIndexOptions(XContentBuilder docBuilder) throws IOException {
        if (testHasIndexOptions == false) {
            return;
        }

        docBuilder.startObject("index_options");
        if (testIndexShouldPrune) {
            docBuilder.field("prune", true);
            docBuilder.startObject("pruning_config");
            {
                docBuilder.field("tokens_freq_ratio_threshold", TEST_PRUNING_TOKENS_FREQ_THRESHOLD);
                docBuilder.field("tokens_weight_threshold", TEST_PRUNING_TOKENS_WEIGHT_THRESHOLD);
            }
            docBuilder.endObject();
        } else {
            docBuilder.field("prune", false);
        }
        docBuilder.endObject();
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

    private SearchResponse performSearch(SearchSourceBuilder source) {
        SearchRequest searchRequest = new SearchRequest(TEST_INDEX_NAME);
        searchRequest.source(source);
        return client().search(searchRequest).actionGet();
    }

    private SearchSourceBuilder getBuilderForSearch() throws IOException {
        boolean shouldUseDefaultTokens = (testQueryShouldNotPrune == false && testHasIndexOptions == false);

        // if we're overriding the index pruning config in the query, always prune
        // if not, and the query should _not_ prune, set prune=false,
        // else, set to `null` to let the index options propagate
        Boolean shouldPrune = overrideQueryPruningConfig ? Boolean.TRUE : (testQueryShouldNotPrune ? Boolean.FALSE : null);
        TokenPruningConfig queryPruningConfig = overrideQueryPruningConfig ? new TokenPruningConfig(3f, 0.5f, true) : null;

        SparseVectorQueryBuilder queryBuilder = new SparseVectorQueryBuilder(
            SPARSE_VECTOR_FIELD,
            shouldUseDefaultTokens ? SEARCH_WEIGHTED_TOKENS_WITH_DEFAULTS : SEARCH_WEIGHTED_TOKENS,
            null,
            null,
            shouldPrune,
            queryPruningConfig
        );

        return new SearchSourceBuilder().query(queryBuilder);
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
            testDescription += " query override prune=false:";
        }

        if (overrideQueryPruningConfig) {
            testDescription += " query override pruningConfig=true:";
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
        new WeightedToken("pugs", 0.5f),
        new WeightedToken("cats", 0.5f),
        new WeightedToken("is", 0.14600334f)
    );

    private static final List<WeightedToken> SEARCH_WEIGHTED_TOKENS_WITH_DEFAULTS = List.of(new WeightedToken("planet", 0.2f));

    private static final List<String> EXPECTED_DOC_IDS_WITHOUT_PRUNING = List.of("3", "2", "1");

    private static final List<String> EXPECTED_DOC_IDS_WITH_PRUNING = List.of();

    private static final List<String> EXPECTED_DOC_IDS_WITH_DEFAULT_PRUNING = List.of("2");

    private static final List<String> EXPECTED_DOC_IDS_WITH_QUERY_OVERRIDE = List.of();
}
