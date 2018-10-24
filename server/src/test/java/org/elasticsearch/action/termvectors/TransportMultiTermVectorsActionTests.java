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

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.TransportMultiGetActionTests;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportMultiTermVectorsActionTests extends ESSingleNodeTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private IndicesService indicesService;
    private TransportService transportService;

    private TransportMultiTermVectorsAction transportMultiTermVectorsAction;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        threadPool = new TestThreadPool(TransportMultiGetActionTests.class.getSimpleName());

        clusterService = getInstanceFromNode(ClusterService.class);
        indicesService = getInstanceFromNode(IndicesService.class);
        transportService = new CapturingTransport().createCapturingTransportService(clusterService.getSettings(), threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR, boundAddress -> clusterService.localNode(), null, emptySet());

        transportService.start();
        transportService.acceptIncomingRequests();

        transportMultiTermVectorsAction = new TransportMultiTermVectorsAction(Settings.EMPTY, transportService, clusterService,
            new TestTransportShardMultiTermsVectorAction(), new ActionFilters(emptySet()), new Resolver());
    }

    @After
    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;

        super.tearDown();
    }

    public void testTransportMultiTermVectorsAction() throws IOException {
        // GIVEN
        final String indexName = createAndPrepareIndex(5, false);

        final Task task = createMultiGetTask();

        final MultiTermVectorsRequestBuilder request = new MultiTermVectorsRequestBuilder(client(), MultiTermVectorsAction.INSTANCE);
        request.add(new TermVectorsRequest(indexName, "type1", "1"));
        request.add(new TermVectorsRequest(indexName, "type1", "2"));

        // WHEN
        transportMultiTermVectorsAction.execute(task, request.request(), new ActionListener<MultiTermVectorsResponse>() {
            // THEN
            @Override
            public void onResponse(MultiTermVectorsResponse multiTermVectorsResponse) {
                try {
                    final MultiTermVectorsItemResponse[] responses = multiTermVectorsResponse.getResponses();

                    assertThat(responses, arrayWithSize(2));

                    assertThat(responses[0].getResponse().getId(), equalTo("1"));
                    assertTrue(responses[0].getResponse().isExists());
                    assertNull(responses[0].getFailure());

                    assertThat(responses[1].getResponse().getId(), equalTo("2"));
                    assertTrue(responses[1].getResponse().isExists());
                    assertNull(responses[1].getFailure());
                } catch (final Exception e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            }

            @Override
            public void onFailure(final Exception e) {
                logger.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
    }

    public void testTransportMultiTermVectorsAction_withMissingRouting() throws IOException {
        // GIVEN
        final String indexName = createAndPrepareIndex(5, true);

        final MultiTermVectorsRequestBuilder request = new MultiTermVectorsRequestBuilder(client(), MultiTermVectorsAction.INSTANCE);
        request.add(new TermVectorsRequest(indexName, "type1", "1").routing("1"));
        request.add(new TermVectorsRequest(indexName, "type1", "2"));

        // WHEN
        transportMultiTermVectorsAction.execute(createMultiGetTask(), request.request(), new ActionListener<MultiTermVectorsResponse>() {
            // THEN
            @Override
            public void onResponse(MultiTermVectorsResponse multiTermVectorsResponse) {
                try {
                    final MultiTermVectorsItemResponse[] responses = multiTermVectorsResponse.getResponses();

                    assertThat(responses, arrayWithSize(2));

                    assertThat(responses[0].getResponse().getId(), equalTo("1"));
                    assertTrue(responses[0].getResponse().isExists());
                    assertNull(responses[0].getFailure());

                    assertNull(responses[1].getResponse());
                    assertThat(responses[1].getFailure().getCause(), instanceOf(RoutingMissingException.class));
                    assertThat(responses[1].getFailure().getCause().getMessage(),
                        equalTo("routing is required for [" + indexName + "]/[type1]/[2]"));
                } catch (final Exception e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            }

            @Override
            public void onFailure(final Exception e) {
                logger.error(e.getMessage(), e);
                fail(e.getMessage());
            }
        });
    }

    private String createAndPrepareIndex(int numberOfDocs, boolean routingRequired) throws IOException {
        final String indexName = randomAlphaOfLength(5).toLowerCase(Locale.getDefault());
        createIndex(indexName, Settings.EMPTY, "type1", "_routing", "required=" + routingRequired, "field1", "type=text");
        ensureGreen(indexName);

        for (int i = 0; i < numberOfDocs; i++) {
            final String id = String.valueOf(i);
            final XContentBuilder source = jsonBuilder().startObject().field("field1", randomAlphaOfLengthBetween(5, 20)).endObject();
            client().prepareIndex(indexName, "type1", id).setSource(source).setRouting(id).setRefreshPolicy(IMMEDIATE).get();
        }

        return indexName;
    }

    private Task createMultiGetTask() {
        return new Task(randomLong(), "transport", MultiGetAction.NAME, "description",
            new TaskId(node().getNodeEnvironment().nodeId() + ":" + randomLong()), emptyMap());
    }

    class TestTransportShardMultiTermsVectorAction extends TransportShardMultiTermsVectorAction {

        TestTransportShardMultiTermsVectorAction() {
            super(Settings.EMPTY, TransportMultiTermVectorsActionTests.this.clusterService,
                TransportMultiTermVectorsActionTests.this.transportService, indicesService,
                TransportMultiTermVectorsActionTests.this.threadPool, new ActionFilters(emptySet()), new Resolver());
        }
    }

    class Resolver extends IndexNameExpressionResolver {

        Resolver() {
            super(Settings.EMPTY);
        }

        @Override
        public String[] concreteIndexNames(ClusterState state, IndicesRequest request) {
            return request.indices();
        }
    }
}
