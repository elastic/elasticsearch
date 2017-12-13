/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorAction;
import org.elasticsearch.xpack.sql.plugin.SqlClearCursorAction.Response;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;
import org.elasticsearch.xpack.sql.session.Cursor;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class SqlClearCursorActionIT extends AbstractSqlIntegTestCase {

    public void testSqlClearCursorAction() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        int indexSize = randomIntBetween(100, 300);
        logger.info("Indexing {} records", indexSize);
        for (int i = 0; i < indexSize; i++) {
            bulkRequestBuilder.add(new IndexRequest("test", "doc", "id" + i).source("data", "bar", "count", i));
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow("test");

        assertEquals(0, getNumberOfSearchContexts());

        int fetchSize = randomIntBetween(5, 20);
        logger.info("Fetching {} records at a time", fetchSize);
        SqlResponse sqlResponse = client().prepareExecute(SqlAction.INSTANCE).query("SELECT * FROM test").fetchSize(fetchSize).get();
        assertEquals(fetchSize, sqlResponse.size());

        assertThat(getNumberOfSearchContexts(), greaterThan(0L));
        assertThat(sqlResponse.cursor(), notNullValue());
        assertThat(sqlResponse.cursor(), not(equalTo(Cursor.EMPTY)));

        Response cleanCursorResponse = client().prepareExecute(SqlClearCursorAction.INSTANCE).cursor(sqlResponse.cursor()).get();
        assertTrue(cleanCursorResponse.isSucceeded());

        assertEquals(0, getNumberOfSearchContexts());
    }

    public void testAutoCursorCleanup() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        int indexSize = randomIntBetween(100, 300);
        logger.info("Indexing {} records", indexSize);
        for (int i = 0; i < indexSize; i++) {
            bulkRequestBuilder.add(new IndexRequest("test", "doc", "id" + i).source("data", "bar", "count", i));
        }
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        ensureYellow("test");

        assertEquals(0, getNumberOfSearchContexts());

        int fetchSize = randomIntBetween(5, 20);
        logger.info("Fetching {} records at a time", fetchSize);
        SqlResponse sqlResponse = client().prepareExecute(SqlAction.INSTANCE).query("SELECT * FROM test").fetchSize(fetchSize).get();
        assertEquals(fetchSize, sqlResponse.size());

        assertThat(getNumberOfSearchContexts(), greaterThan(0L));
        assertThat(sqlResponse.cursor(), notNullValue());
        assertThat(sqlResponse.cursor(), not(equalTo(Cursor.EMPTY)));

        long fetched = sqlResponse.size();
        do {
            sqlResponse = client().prepareExecute(SqlAction.INSTANCE).cursor(sqlResponse.cursor()).get();
            fetched += sqlResponse.size();
        } while (sqlResponse.cursor().equals(Cursor.EMPTY) == false);
        assertEquals(indexSize, fetched);

        Response cleanCursorResponse = client().prepareExecute(SqlClearCursorAction.INSTANCE).cursor(sqlResponse.cursor()).get();
        assertFalse(cleanCursorResponse.isSucceeded());

        assertEquals(0, getNumberOfSearchContexts());
    }

    private long getNumberOfSearchContexts() {
        return client().admin().indices().prepareStats("test").clear().setSearch(true).get()
                .getIndex("test").getTotal().getSearch().getOpenContexts();
    }
}
