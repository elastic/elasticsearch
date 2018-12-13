/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.sql.proto.ColumnInfo;
import org.elasticsearch.xpack.sql.proto.Mode;

import java.sql.Types;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SqlActionIT extends AbstractSqlIntegTestCase {

    public void testSqlAction() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        client().prepareBulk()
                .add(new IndexRequest("test", "doc", "1").source("data", "bar", "count", 42))
                .add(new IndexRequest("test", "doc", "2").source("data", "baz", "count", 43))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        ensureYellow("test");

        boolean dataBeforeCount = randomBoolean();
        String columns = dataBeforeCount ? "data, count" : "count, data";
        SqlQueryResponse response = new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE)
                .query("SELECT " + columns + " FROM test ORDER BY count").mode(Mode.JDBC).get();
        assertThat(response.size(), equalTo(2L));
        assertThat(response.columns(), hasSize(2));
        int dataIndex = dataBeforeCount ? 0 : 1;
        int countIndex = dataBeforeCount ? 1 : 0;
        assertEquals(new ColumnInfo("", "data", "text", Types.VARCHAR, 0), response.columns().get(dataIndex));
        assertEquals(new ColumnInfo("", "count", "long", Types.BIGINT, 20), response.columns().get(countIndex));

        assertThat(response.rows(), hasSize(2));
        assertEquals("bar", response.rows().get(0).get(dataIndex));
        assertEquals(42L, response.rows().get(0).get(countIndex));
        assertEquals("baz", response.rows().get(1).get(dataIndex));
        assertEquals(43L, response.rows().get(1).get(countIndex));
    }
}

