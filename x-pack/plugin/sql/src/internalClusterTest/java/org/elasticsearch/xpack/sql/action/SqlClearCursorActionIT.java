/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.sql.session.Cursor;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class SqlClearCursorActionIT extends AbstractSqlIntegTestCase {

    public void testSqlClearCursorAction() {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        int indexSize = randomIntBetween(100, 300);
        logger.info("Indexing {} records", indexSize);
        for (int i = 0; i < indexSize; i++) {
            bulkRequestBuilder.add(new IndexRequest("test").id("id" + i).source("data", "bar", "count", i));
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow("test");

        assertEquals(0, getNumberOfSearchContexts());

        int fetchSize = randomIntBetween(5, 20);
        logger.info("Fetching {} records at a time", fetchSize);
        SqlQueryResponse sqlQueryResponse = new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).query("SELECT * FROM test")
            .fetchSize(fetchSize)
            .get();
        assertEquals(fetchSize, sqlQueryResponse.size());

        assertThat(getNumberOfSearchContexts(), greaterThan(0L));
        assertThat(sqlQueryResponse.cursor(), notNullValue());
        assertThat(sqlQueryResponse.cursor(), not(equalTo(Cursor.EMPTY)));

        SqlClearCursorResponse cleanCursorResponse = new SqlClearCursorRequestBuilder(client(), SqlClearCursorAction.INSTANCE).cursor(
            sqlQueryResponse.cursor()
        ).get();
        assertTrue(cleanCursorResponse.isSucceeded());

        assertEquals(0, getNumberOfSearchContexts());
    }

    public void testAutoCursorCleanup() {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        int indexSize = randomIntBetween(100, 300);
        logger.info("Indexing {} records", indexSize);
        for (int i = 0; i < indexSize; i++) {
            bulkRequestBuilder.add(new IndexRequest("test").id("id" + i).source("data", "bar", "count", i));
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow("test");

        assertEquals(0, getNumberOfSearchContexts());

        int fetchSize = randomIntBetween(5, 20);
        logger.info("Fetching {} records at a time", fetchSize);
        SqlQueryResponse sqlQueryResponse = new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).query("SELECT * FROM test")
            .fetchSize(fetchSize)
            .get();
        assertEquals(fetchSize, sqlQueryResponse.size());

        assertThat(getNumberOfSearchContexts(), greaterThan(0L));
        assertThat(sqlQueryResponse.cursor(), notNullValue());
        assertThat(sqlQueryResponse.cursor(), not(equalTo(Cursor.EMPTY)));

        long fetched = sqlQueryResponse.size();
        do {
            sqlQueryResponse = new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).cursor(sqlQueryResponse.cursor()).get();
            fetched += sqlQueryResponse.size();
        } while (sqlQueryResponse.cursor().isEmpty() == false);
        assertEquals(indexSize, fetched);

        SqlClearCursorResponse cleanCursorResponse = new SqlClearCursorRequestBuilder(client(), SqlClearCursorAction.INSTANCE).cursor(
            sqlQueryResponse.cursor()
        ).get();
        assertFalse(cleanCursorResponse.isSucceeded());

        assertEquals(0, getNumberOfSearchContexts());
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
