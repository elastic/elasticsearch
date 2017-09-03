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

package org.elasticsearch.action.search;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.test.ESIntegTestCase;

public class CanMatchIT extends ESIntegTestCase {

    /*
     * Previously the can match phase executed on the same thread pool. In cases that a query coordinating node held all the shards for a
     * query, the can match phase would recurse and end in stack overflow (and this thread would be a networking thread, tying such a thread
     * up for an extended period of time). This test is for that situation.
     */
    public void testAvoidStackOverflow() throws InterruptedException {
        final String node = internalCluster().startDataOnlyNode(Settings.builder().put("node.attr.color", "blue").build());
        /*
         * While 640 shards for a single index is excessive, creating one index with 640 shards is cheaper than creating 128 indices with 5
         * shards yet both set up the failing reproduction that we need.
         */
        final Settings.Builder settings =
                Settings.builder().put("index.routing.allocation.include.color", "blue").put("index.number_of_shards", 640);
        final CreateIndexRequest createIndexRequest =
                new CreateIndexRequest("index").settings(settings);
        client().admin().indices().create(createIndexRequest).actionGet();
        ensureGreen();
        // we have to query through the data node so that all requests are local; if this query executes successfully, the test passes
        client(node).prepareSearch("index").setQuery(new QueryStringQueryBuilder("")).execute().actionGet();
    }

}
