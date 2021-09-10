/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.basic;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;


/**
 * This integration test contains two tests to provoke a search failure while initializing a new empty index:
 * <ul>
 *     <li>testSearchDuringCreate: just tries to create an index and search, has low likelihood for provoking issue (but it does happen)
 *     </li>
 *     <li>testDelayIsolatedPrimary: delays network messages from all nodes except search coordinator to primary, ensuring that every test
 *     run hits the case where a primary is initializing a newly created shard</li>
 * </ul>
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class SearchWhileInitializingEmptyIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testSearchDuringCreate() {
        ActionFuture<CreateIndexResponse> createFuture = prepareCreate("test").execute();

        for (int i = 0; i < 100; ++i) {
            SearchResponse searchResponse = client().prepareSearch("test*").setAllowPartialSearchResults(randomBoolean()).get();
            assertThat(searchResponse.getFailedShards(), equalTo(0));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
        }

        logger.info("done searching");
        assertAcked(createFuture.actionGet());
    }

    public void testDelayIsolatedPrimary() throws Exception {
        String[] originalNodes = internalCluster().getNodeNames();
        String dataNode = internalCluster().startDataOnlyNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        NetworkDisruption.Bridge bridge = new NetworkDisruption.Bridge(coordinatorNode, Set.of(dataNode), Set.of(originalNodes));
        NetworkDisruption scheme =
            new NetworkDisruption(bridge, new NetworkDisruption.NetworkDelay(NetworkDisruption.NetworkDelay.DEFAULT_DELAY_MIN));
        setDisruptionScheme(scheme);
        scheme.startDisrupting();

        ActionFuture<CreateIndexResponse> createFuture;
        try {
            Settings.Builder builder = Settings.builder().put(indexSettings())
                .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + "._name", dataNode);

            createFuture =
                internalCluster().masterClient().admin().indices()
                    .create(new CreateIndexRequest("test", builder.build()).timeout(TimeValue.ZERO));

            // wait for available on coordinator
            assertBusy(() -> {
                try {
                    client(coordinatorNode).get(new GetRequest("test", "0")).actionGet();
                    throw new IllegalStateException("non-assertion exception to escape assertBusy, get request must fail");
                } catch (IndexNotFoundException e) {
                    throw new AssertionError(e);
                } catch (NoShardAvailableActionException e) {
                    // now coordinator knows about the index.
                }
            });

            // not available on data node.
            ElasticsearchException exception = expectThrows(ElasticsearchException.class, () ->
                client(dataNode).get(new GetRequest("test", "0")).actionGet());
            assertThat(exception, anyOf(instanceOf(NoShardAvailableActionException.class), instanceOf(IndexNotFoundException.class)));

            for (String indices : new String[] {"test*", "tes*", "test"}){
                logger.info("Searching for [{}]", indices);
                SearchResponse searchResponse =
                    client(coordinatorNode).prepareSearch(indices).setAllowPartialSearchResults(randomBoolean()).get();
                assertThat(searchResponse.getFailedShards(), equalTo(0));
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
            }
        } finally {
            internalCluster().clearDisruptionScheme(true);
        }
        createFuture.actionGet();
    }
}
