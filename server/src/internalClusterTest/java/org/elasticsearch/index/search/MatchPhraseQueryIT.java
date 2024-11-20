/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.ZeroTermsQueryOption;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class MatchPhraseQueryIT extends ESIntegTestCase {
    private static final String INDEX = "test";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        CreateIndexRequestBuilder createIndexRequest = prepareCreate(INDEX).setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.analysis.analyzer.standard_stopwords.type", "standard")
                .putList("index.analysis.analyzer.standard_stopwords.stopwords", "of", "the", "who")
        );
        assertAcked(createIndexRequest);
        ensureGreen();
    }

    public void testZeroTermsQuery() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = getIndexRequests();
        indexRandom(true, false, indexRequests);

        MatchPhraseQueryBuilder baseQuery = matchPhraseQuery("name", "the who").analyzer("standard_stopwords");

        MatchPhraseQueryBuilder matchNoneQuery = baseQuery.zeroTermsQuery(ZeroTermsQueryOption.NONE);
        assertHitCount(prepareSearch(INDEX).setQuery(matchNoneQuery), 0L);

        MatchPhraseQueryBuilder matchAllQuery = baseQuery.zeroTermsQuery(ZeroTermsQueryOption.ALL);
        assertHitCount(prepareSearch(INDEX).setQuery(matchAllQuery), 2L);
    }

    private List<IndexRequestBuilder> getIndexRequests() {
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(prepareIndex(INDEX).setSource("name", "the beatles"));
        requests.add(prepareIndex(INDEX).setSource("name", "led zeppelin"));
        return requests;
    }
}
