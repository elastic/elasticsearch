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

package org.elasticsearch.transport;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.bench.AbortBenchmarkAction;
import org.elasticsearch.action.bench.BenchmarkAction;
import org.elasticsearch.action.bench.BenchmarkService;
import org.elasticsearch.action.bench.BenchmarkStatusAction;
import org.elasticsearch.action.exists.ExistsAction;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptAction;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.test.ElasticsearchBackwardsCompatIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.*;

public class ActionNamesBackwardsCompatibilityTest extends ElasticsearchBackwardsCompatIntegrationTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testTransportHandlers() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        InternalTestCluster internalCluster = backwardsCluster().internalCluster();
        TransportService transportService = internalCluster.getInstance(TransportService.class);
        ImmutableMap<String, TransportRequestHandler> requestHandlers = transportService.serverHandlers;

        DiscoveryNodes nodes = client().admin().cluster().prepareState().get().getState().nodes();

        DiscoveryNode selectedNode = null;
        for (DiscoveryNode node : nodes) {
            if (node.getVersion().before(Version.CURRENT)) {
                selectedNode = node;
                break;
            }
        }
        assertThat(selectedNode, notNullValue());

        final TransportRequest transportRequest = new TransportRequest() {};

        for (String action : requestHandlers.keySet()) {

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<TransportException> failure = new AtomicReference<>();
            transportService.sendRequest(selectedNode, action, transportRequest, new TransportResponseHandler<TransportResponse>() {
                @Override
                public TransportResponse newInstance() {
                    return new TransportResponse() {};
                }

                @Override
                public void handleResponse(TransportResponse response) {
                    latch.countDown();
                }

                @Override
                public void handleException(TransportException exp) {
                    failure.set(exp);
                    latch.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
            assertThat(latch.await(5, TimeUnit.SECONDS), equalTo(true));

            if (failure.get() != null) {
                Throwable cause = failure.get().unwrapCause();
                if (isActionNotFoundExpected(selectedNode.version(), action)) {
                    assertThat(cause, instanceOf(ActionNotFoundTransportException.class));
                } else {
                    assertThat(cause, not(instanceOf(ActionNotFoundTransportException.class)));
                    if (! (cause instanceof IndexOutOfBoundsException)) {
                        cause.printStackTrace();
                    }
                }
            }
        }
    }

    private static boolean isActionNotFoundExpected(Version version, String action) {
        Version actionVersion = actionsVersions.get(action);
        return actionVersion != null && version.before(actionVersion);
    }

    private static final Map<String, Version> actionsVersions = new HashMap<>();

    static {
        actionsVersions.put(BenchmarkService.STATUS_ACTION_NAME, Version.V_1_4_0_Beta1);
        actionsVersions.put(BenchmarkService.START_ACTION_NAME, Version.V_1_4_0_Beta1);
        actionsVersions.put(BenchmarkService.ABORT_ACTION_NAME, Version.V_1_4_0_Beta1);
        actionsVersions.put(BenchmarkAction.NAME, Version.V_1_4_0_Beta1);
        actionsVersions.put(BenchmarkStatusAction.NAME, Version.V_1_4_0_Beta1);
        actionsVersions.put(AbortBenchmarkAction.NAME, Version.V_1_4_0_Beta1);
        actionsVersions.put(GetIndexAction.NAME, Version.V_1_4_0_Beta1);

        actionsVersions.put(ExistsAction.NAME, Version.V_1_4_0_Beta1);
        actionsVersions.put(ExistsAction.NAME + "[s]", Version.V_1_4_0_Beta1);

        actionsVersions.put(IndicesStore.ACTION_SHARD_EXISTS, Version.V_1_3_0);

        actionsVersions.put(GetIndexedScriptAction.NAME, Version.V_1_3_0);
        actionsVersions.put(DeleteIndexedScriptAction.NAME, Version.V_1_3_0);
        actionsVersions.put(PutIndexedScriptAction.NAME, Version.V_1_3_0);

        actionsVersions.put(SearchServiceTransportAction.FREE_CONTEXT_SCROLL_ACTION_NAME, Version.V_1_4_0_Beta1);
        actionsVersions.put(SearchServiceTransportAction.FETCH_ID_SCROLL_ACTION_NAME, Version.V_1_4_0_Beta1);
    }
}
