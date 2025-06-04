/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams.lifecycle;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.features.ResetFeatureStateResponse.ResetFeatureStateStatus;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate.DataStreamTemplate;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemDataStreamDescriptor;
import org.elasticsearch.indices.SystemDataStreamDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CrudSystemDataStreamLifecycleIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, TestSystemDataStreamPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
            .build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void testDataLifecycleOnSystemDataStream() throws Exception {
        String systemDataStream = ".test-data-stream";
        RequestOptions correctProductHeader = RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "product").build();
        RequestOptions wrongProductHeader = RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "wrong").build();
        try (RestClient restClient = createRestClient()) {
            // Set-up system data stream
            {
                Request putRequest = new Request("PUT", "/_data_stream/" + systemDataStream);
                putRequest.setOptions(correctProductHeader);
                Response putResponse = restClient.performRequest(putRequest);
                assertThat(putResponse.getStatusLine().getStatusCode(), is(200));
            }

            // data stream lifecycle of hidden data streams is not retrieved by default
            {
                Request listAllVisibleRequest = new Request("GET", "/_data_stream/*/_lifecycle");
                Response listAllVisibleResponse = restClient.performRequest(listAllVisibleRequest);
                assertThat(listAllVisibleResponse.getStatusLine().getStatusCode(), is(200));
                Map<String, Object> visibleResponseMap = XContentHelper.convertToMap(
                    XContentType.JSON.xContent(),
                    EntityUtils.toString(listAllVisibleResponse.getEntity()),
                    false
                );
                List<Object> visibleDataStreams = (List<Object>) visibleResponseMap.get("data_streams");
                assertThat(visibleDataStreams.size(), is(0));
            }

            // data stream lifecycle of hidden data streams is retrieved when enabled - no header needed
            {
                Request listAllRequest = new Request("GET", "/_data_stream/*/_lifecycle");
                listAllRequest.addParameter("expand_wildcards", "open,hidden");
                Response listAllResponse = restClient.performRequest(listAllRequest);
                assertThat(listAllResponse.getStatusLine().getStatusCode(), is(200));
                Map<String, Object> responseMap = XContentHelper.convertToMap(
                    XContentType.JSON.xContent(),
                    EntityUtils.toString(listAllResponse.getEntity()),
                    false
                );
                List<Object> dataStreams = (List<Object>) responseMap.get("data_streams");
                assertThat(dataStreams.size(), is(1));
                Map<String, Object> dataStreamLifecycle = (Map<String, Object>) dataStreams.get(0);
                assertThat(dataStreamLifecycle.get("name"), equalTo(systemDataStream));
            }

            // Retrieve using the concrete data stream name - header needed
            {
                Request listRequest = new Request("GET", "/_data_stream/" + systemDataStream + "/_lifecycle");
                Response listResponse = restClient.performRequest(listRequest);
                assertThat(listResponse.getStatusLine().getStatusCode(), is(200));
                Map<String, Object> responseMap = XContentHelper.convertToMap(
                    XContentType.JSON.xContent(),
                    EntityUtils.toString(listResponse.getEntity()),
                    false
                );
                List<Object> dataStreams = (List<Object>) responseMap.get("data_streams");
                assertThat(dataStreams.size(), is(1));
            }

            // Update the lifecycle
            {
                Request putRequest = new Request("PUT", "/_data_stream/" + systemDataStream + "/_lifecycle");
                putRequest.setJsonEntity("""
                    {
                    }""");
                // No header
                ResponseException re = expectThrows(ResponseException.class, () -> restClient.performRequest(putRequest));
                assertThat(re.getMessage(), containsString("reserved for system"));

                // wrong header
                putRequest.setOptions(wrongProductHeader);
                re = expectThrows(ResponseException.class, () -> restClient.performRequest(putRequest));
                assertThat(re.getMessage(), containsString("may not be accessed by product [wrong]"));

                // correct
                putRequest.setOptions(correctProductHeader);
                Response putResponse = restClient.performRequest(putRequest);
                assertThat(putResponse.getStatusLine().getStatusCode(), is(200));
            }

            // delete
            {
                Request deleteRequest = new Request("DELETE", "/_data_stream/" + systemDataStream + "/_lifecycle");
                ResponseException re = expectThrows(ResponseException.class, () -> restClient.performRequest(deleteRequest));
                assertThat(re.getMessage(), containsString("reserved for system"));

                // wrong header
                deleteRequest.setOptions(wrongProductHeader);
                re = expectThrows(ResponseException.class, () -> restClient.performRequest(deleteRequest));
                assertThat(re.getMessage(), containsString("may not be accessed by product [wrong]"));

                // correct
                deleteRequest.setOptions(correctProductHeader);
                Response deleteResponse = restClient.performRequest(deleteRequest);
                assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
            }
        }
    }

    @After
    public void cleanup() {
        try {
            PlainActionFuture<ResetFeatureStateStatus> stateStatusPlainActionFuture = new PlainActionFuture<>();
            new TestSystemDataStreamPlugin().cleanUpFeature(
                internalCluster().clusterService(),
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                internalCluster().client(),
                stateStatusPlainActionFuture
            );
            stateStatusPlainActionFuture.actionGet();
        } catch (ResourceNotFoundException e) {
            // ignore
        }
    }

    public static final class TestSystemDataStreamPlugin extends Plugin implements SystemIndexPlugin {

        @Override
        public Collection<SystemDataStreamDescriptor> getSystemDataStreamDescriptors() {
            try {
                CompressedXContent mappings = new CompressedXContent("{\"properties\":{\"name\":{\"type\":\"keyword\"}}}");
                return List.of(
                    new SystemDataStreamDescriptor(
                        ".test-data-stream",
                        "system data stream test",
                        Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(".test-data-stream"))
                            .template(
                                Template.builder()
                                    .settings(Settings.EMPTY)
                                    .mappings(mappings)
                                    .lifecycle(DataStreamLifecycle.dataLifecycleBuilder().dataRetention(randomPositiveTimeValue()))
                            )
                            .dataStreamTemplate(new DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of("product"),
                        "product",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    )
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public String getFeatureName() {
            return CrudSystemDataStreamLifecycleIT.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "Integration testing of modifying the data stream lifecycle of system data streams";
        }

        @Override
        public void cleanUpFeature(
            ClusterService clusterService,
            ProjectResolver projectResolver,
            Client client,
            ActionListener<ResetFeatureStateStatus> listener
        ) {
            Collection<SystemDataStreamDescriptor> dataStreamDescriptors = getSystemDataStreamDescriptors();
            final DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                dataStreamDescriptors.stream().map(SystemDataStreamDescriptor::getDataStreamName).toList().toArray(Strings.EMPTY_ARRAY)
            );
            request.indicesOptions(
                IndicesOptions.builder(request.indicesOptions())
                    .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
                    .build()
            );
            try {
                client.execute(
                    DeleteDataStreamAction.INSTANCE,
                    request,
                    ActionListener.wrap(
                        response -> SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, listener),
                        e -> {
                            Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                            if (unwrapped instanceof ResourceNotFoundException) {
                                SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, listener);
                            } else {
                                listener.onFailure(e);
                            }
                        }
                    )
                );
            } catch (Exception e) {
                Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                if (unwrapped instanceof ResourceNotFoundException) {
                    SystemIndexPlugin.super.cleanUpFeature(clusterService, projectResolver, client, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        }
    }
}
