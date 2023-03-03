/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.broadcast;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class BroadcastActionsIT extends ESIntegTestCase {

    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    public void testBroadcastOperations() throws IOException {
        assertAcked(prepareCreate("test", 1));

        NumShards numShards = getNumShards("test");

        logger.info("Running Cluster Health");
        client().index(new IndexRequest("test").id("1").source(source("1", "test"))).actionGet();
        flush();
        client().index(new IndexRequest("test").id("2").source(source("2", "test"))).actionGet();
        refresh();

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            SearchResponse countResponse = client().prepareSearch("test").setSize(0).setQuery(matchAllQuery()).get();
            assertThat(countResponse.getHits().getTotalHits().value, equalTo(2L));
            assertThat(countResponse.getTotalShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }
    }

    private XContentBuilder source(String id, String nameValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
