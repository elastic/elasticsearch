/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.document;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(numDataNodes = 1, scope = Scope.SUITE)
public class BulkNoAutoCreateIndexTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder().put(super.nodeSettings(nodeOrdinal)).put("action.auto_create_index", false).build();
    }

    @Test // issue 6410
    public void testThatMissingIndexDoesNotAbortFullBulkRequest() throws Exception {
        createIndex("bulkindex1", "bulkindex2");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("bulkindex1", "index1_type", "1").source("text", "hallo1"))
                .add(new IndexRequest("bulkindex2", "index2_type", "1").source("text", "hallo2"))
                .add(new IndexRequest("bulkindex2", "index2_type").source("text", "hallo2"))
                .add(new UpdateRequest("bulkindex2", "index2_type", "2").doc("foo", "bar"))
                .add(new DeleteRequest("bulkindex2", "index2_type", "3"))
                .refresh(true);

        client().bulk(bulkRequest).get();
        SearchResponse searchResponse = client().prepareSearch("bulkindex*").get();
        assertHitCount(searchResponse, 3);

        assertAcked(client().admin().indices().prepareClose("bulkindex2"));

        BulkResponse bulkResponse = client().bulk(bulkRequest).get();
        assertThat(bulkResponse.hasFailures(), is(true));
        assertThat(bulkResponse.getItems().length, is(5));

    }

}
