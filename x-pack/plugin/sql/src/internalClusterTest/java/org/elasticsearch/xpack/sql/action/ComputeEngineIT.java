/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.xpack.sql.proto.Mode;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ComputeEngineIT extends AbstractSqlIntegTestCase {

    public void testComputeEngine() {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        for (int i = 0; i < 10; i++) {
            client().prepareBulk()
                .add(new IndexRequest("test").id("1" + i).source("data", "bar", "count", 42))
                .add(new IndexRequest("test").id("2" + i).source("data", "baz", "count", 43))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }
        ensureYellow("test");

        SqlQueryResponse response = new SqlQueryRequestBuilder(client(), SqlQueryAction.INSTANCE).query(
            "SELECT data, AVG(count) FROM test GROUP BY data"
        ).mode(Mode.JDBC).version(Version.CURRENT.toString()).get();
        assertThat(response.size(), equalTo(2L)); // fails as we're not extracting responses
        assertThat(response.columns(), hasSize(2));

        assertThat(response.rows(), hasSize(2));
    }
}
