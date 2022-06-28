/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchService;

import java.util.Arrays;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

public class SqlSearchPageTimeoutIT extends AbstractSqlIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // use static low keepAlive interval to ensure obsolete search contexts are pruned soon enough
        settings.put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(100));
        return settings.build();
    }

    public void testSearchContextIsCleanedUpAfterPageTimeoutForHitsQueries() throws Exception {
        setupTestIndex();

        SqlQueryResponse response = new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).query("SELECT field FROM test")
            .fetchSize(1)
            .pageTimeout(TimeValue.timeValueMillis(500))
            .get();

        assertTrue(response.hasCursor());
        assertEquals(1, getNumberOfSearchContexts());

        assertBusy(() -> assertEquals(0, getNumberOfSearchContexts()));

        SearchPhaseExecutionException exception = expectThrows(
            SearchPhaseExecutionException.class,
            () -> new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).cursor(response.cursor()).get()
        );

        assertThat(Arrays.asList(exception.guessRootCauses()), contains(instanceOf(SearchContextMissingException.class)));
    }

    public void testNoSearchContextForAggregationQueries() throws InterruptedException {
        setupTestIndex();

        SqlQueryResponse response = new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).query(
            "SELECT COUNT(*) FROM test GROUP BY field"
        ).fetchSize(1).pageTimeout(TimeValue.timeValueMillis(500)).get();

        assertEquals(1, response.size());
        assertTrue(response.hasCursor());
        assertEquals(0, getNumberOfSearchContexts());

        Thread.sleep(1000);

        // since aggregation queries do not have a stateful search context, scrolling is still possible after page_timeout
        response = new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).cursor(response.cursor()).get();

        assertEquals(1, response.size());
    }

    private void setupTestIndex() {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("field", "bar"))
            .add(new IndexRequest("test").id("2").source("field", "baz"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("test");
    }

    private long getNumberOfSearchContexts() {
        return client().admin()
            .indices()
            .prepareStats("test")
            .clear()
            .setSearch(true)
            .get()
            .getIndex("test")
            .getTotal()
            .getSearch()
            .getOpenContexts();
    }
}
