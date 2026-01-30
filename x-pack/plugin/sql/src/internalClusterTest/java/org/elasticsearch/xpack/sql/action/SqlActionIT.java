/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersions;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SqlActionIT extends AbstractSqlIntegTestCase {

    public void testSqlAction() {
        assertAcked(indicesAdmin().prepareCreate("test").get());
        client().prepareBulk()
            .add(new IndexRequest("test").id("1").source("data", "bar", "count", 42))
            .add(new IndexRequest("test").id("2").source("data", "baz", "count", 43))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow("test");

        boolean dataBeforeCount = randomBoolean();
        String columns = dataBeforeCount ? "data, count" : "count, data";
        SqlQueryResponse response = new SqlQueryRequestBuilder(client()).query("SELECT " + columns + " FROM test ORDER BY count")
            .mode(Mode.JDBC)
            .version(SqlVersions.SERVER_COMPAT_VERSION.toString())
            .get();
        assertThat(response.size(), equalTo(2L));
        assertThat(response.columns(), hasSize(2));
        int dataIndex = dataBeforeCount ? 0 : 1;
        int countIndex = dataBeforeCount ? 1 : 0;
        assertEquals(new ColumnInfo("", "data", "text", 2147483647), response.columns().get(dataIndex));
        assertEquals(new ColumnInfo("", "count", "long", 20), response.columns().get(countIndex));

        assertThat(response.rows(), hasSize(2));
        assertEquals("bar", response.rows().get(0).get(dataIndex));
        assertEquals(42L, response.rows().get(0).get(countIndex));
        assertEquals("baz", response.rows().get(1).get(dataIndex));
        assertEquals(43L, response.rows().get(1).get(countIndex));
    }

    public void testSqlActionCurrentVersion() {
        SqlQueryResponse response = new SqlQueryRequestBuilder(client()).query("SELECT true")
            .mode(randomFrom(Mode.CLI, Mode.JDBC))
            .version(SqlVersions.SERVER_COMPAT_VERSION.toString())
            .get();
        assertThat(response.size(), equalTo(1L));
        assertEquals(true, response.rows().get(0).get(0));
    }

    public void testSqlActionOutdatedVersion() {
        SqlQueryRequestBuilder request = new SqlQueryRequestBuilder(client()).query("SELECT true")
            .mode(randomFrom(Mode.CLI, Mode.JDBC))
            .version("1.2.3");
        expectThrows(org.elasticsearch.action.ActionRequestValidationException.class, request);
    }
}
