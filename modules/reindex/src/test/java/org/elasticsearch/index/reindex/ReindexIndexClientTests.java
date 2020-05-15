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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.reindex.ReindexIndexClient.REINDEX_ALIAS;
import static org.elasticsearch.index.reindex.ReindexIndexClient.REINDEX_INDEX_7;
import static org.hamcrest.Matchers.equalTo;

public class ReindexIndexClientTests extends ESSingleNodeTestCase {

    public void testAliasAndIndexCreated() {
        ReindexIndexClient client = new ReindexIndexClient(client(), getInstanceFromNode(ClusterService.class), null);

        PlainActionFuture<ReindexTaskState> future = new PlainActionFuture<>();
        long startMillis = Instant.now().toEpochMilli();
        ReindexTaskStateDoc reindexState = new ReindexTaskStateDoc(new ReindexRequest(), randomBoolean(), startMillis);
        client.createReindexTaskDoc(randomAlphaOfLength(5), reindexState, future);
        future.actionGet(10, TimeUnit.SECONDS);

        GetAliasesResponse aliases = client().admin().indices().prepareGetAliases(REINDEX_ALIAS).get();
        assertTrue(aliases.getAliases().containsKey(REINDEX_INDEX_7));
        assertThat(aliases.getAliases().size(), equalTo(1));
        assertThat(aliases.getAliases().get(REINDEX_INDEX_7).size(), equalTo(1));
        assertThat(aliases.getAliases().get(REINDEX_INDEX_7).get(0).alias(), equalTo(REINDEX_ALIAS));
    }
}
