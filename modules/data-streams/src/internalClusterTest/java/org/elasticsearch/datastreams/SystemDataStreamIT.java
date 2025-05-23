/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.datastreams;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
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
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.MetadataIndexTemplateService.DEFAULT_TIMESTAMP_FIELD;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class SystemDataStreamIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(DataStreamsPlugin.class);
        plugins.add(TestSystemDataStreamPlugin.class);
        plugins.add(MapperExtrasPlugin.class);
        return plugins;
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
    public void testSystemDataStreamCRUD() throws Exception {
        try (RestClient restClient = createRestClient()) {
            Request putRequest = new Request("PUT", "/_data_stream/.test-data-stream");

            // no product header
            ResponseException re = expectThrows(ResponseException.class, () -> restClient.performRequest(putRequest));
            assertThat(re.getMessage(), containsString("reserved for system"));

            // wrong header
            putRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "wrong").build());
            re = expectThrows(ResponseException.class, () -> restClient.performRequest(putRequest));
            assertThat(re.getMessage(), containsString("accessed by product"));

            // correct
            putRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "product").build());
            Response putResponse = restClient.performRequest(putRequest);
            assertThat(putResponse.getStatusLine().getStatusCode(), is(200));

            // system data streams are hidden
            Request listAllVisibleRequest = new Request("GET", "/_data_stream");
            Response listAllVisibleResponse = restClient.performRequest(listAllVisibleRequest);
            assertThat(listAllVisibleResponse.getStatusLine().getStatusCode(), is(200));
            Map<String, Object> visibleResponseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(listAllVisibleResponse.getEntity()),
                false
            );
            List<Object> visibleDataStreams = (List<Object>) visibleResponseMap.get("data_streams");
            assertThat(visibleDataStreams.size(), is(0));

            // list - no header needed
            Request listAllRequest = new Request("GET", "/_data_stream");
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

            Request listRequest = new Request("GET", "/_data_stream/.test-data-stream");
            Response listResponse = restClient.performRequest(listRequest);
            assertThat(listResponse.getStatusLine().getStatusCode(), is(200));
            responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), EntityUtils.toString(listResponse.getEntity()), false);
            dataStreams = (List<Object>) responseMap.get("data_streams");
            assertThat(dataStreams.size(), is(1));

            // delete
            Request deleteRequest = new Request("DELETE", "/_data_stream/.test-data-stream");

            // no header
            re = expectThrows(ResponseException.class, () -> restClient.performRequest(deleteRequest));
            assertThat(re.getMessage(), containsString("reserved for system"));

            // incorrect header
            deleteRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "wrong").build());
            re = expectThrows(ResponseException.class, () -> restClient.performRequest(deleteRequest));
            assertThat(re.getMessage(), containsString("accessed by product"));

            // correct
            deleteRequest.setOptions(putRequest.getOptions());
            Response deleteResponse = restClient.performRequest(deleteRequest);
            assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
        }
    }

    public void testDataStreamStats() throws Exception {
        try (RestClient restClient = createRestClient()) {
            Request putRequest = new Request("PUT", "/_data_stream/.test-data-stream");
            putRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "product").build());
            Response putResponse = restClient.performRequest(putRequest);
            assertThat(putResponse.getStatusLine().getStatusCode(), is(200));

            Request statsRequest = new Request("GET", "/_data_stream/_stats");
            statsRequest.addParameter("expand_wildcards", "open,hidden");
            Response response = restClient.performRequest(statsRequest);
            assertThat(response.getStatusLine().getStatusCode(), is(200));

            Map<String, Object> map = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(response.getEntity()),
                false
            );
            assertThat(map.get("data_stream_count"), equalTo(1));
        }
    }

    public void testSystemDataStreamWithFailureStore() throws Exception {
        String dataStreamName = ".test-failure-store";
        RequestOptions productHeader = RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "product").build();
        try (RestClient restClient = createRestClient()) {
            Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
            indexRequest.setOptions(productHeader);
            String value = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(System.currentTimeMillis());
            indexRequest.setJsonEntity(
                String.format(Locale.ROOT, "{\"%s\":\"%s\",\"count\":\"not-a-number\"}", DEFAULT_TIMESTAMP_FIELD, value)
            );

            Response indexResponse = restClient.performRequest(indexRequest);
            assertThat(indexResponse.getStatusLine().getStatusCode(), is(201));
            Map<String, Object> responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(indexResponse.getEntity()),
                false
            );
            assertThat(responseMap.get("result"), equalTo("created"));
            assertThat((String) responseMap.get("_index"), startsWith(DataStream.FAILURE_STORE_PREFIX));
            assertThat(responseMap.get("failure_store"), equalTo("used"));

            // Rollover
            Request rolloverRequest = new Request("POST", "/" + dataStreamName + "::failures/_rollover");
            rolloverRequest.setOptions(productHeader);
            Response rolloverResponse = restClient.performRequest(rolloverRequest);
            assertThat(rolloverResponse.getStatusLine().getStatusCode(), is(200));
            responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(rolloverResponse.getEntity()),
                false
            );
            assertThat(responseMap.get("acknowledged"), equalTo(true));
            assertThat(responseMap.get("rolled_over"), equalTo(true));
            assertThat((String) responseMap.get("new_index"), startsWith(DataStream.FAILURE_STORE_PREFIX));

            // Edit data stream options
            Request editOptionsRequest = new Request("PUT", "/_data_stream/" + dataStreamName + "/_options");
            editOptionsRequest.setJsonEntity("{\"failure_store\":{\"enabled\":\"false\"}}");
            editOptionsRequest.setOptions(productHeader);
            Response editOptionsResponse = restClient.performRequest(editOptionsRequest);
            assertThat(editOptionsResponse.getStatusLine().getStatusCode(), is(200));
            responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(editOptionsResponse.getEntity()),
                false
            );
            assertThat(responseMap.get("acknowledged"), equalTo(true));

            // delete
            Request deleteRequest = new Request("DELETE", "/_data_stream/" + dataStreamName);
            deleteRequest.setOptions(productHeader);
            Response deleteResponse = restClient.performRequest(deleteRequest);
            assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
        }
    }

    @SuppressWarnings("unchecked")
    public void testSystemDataStreamReadWrite() throws Exception {
        try (RestClient restClient = createRestClient()) {
            Request putRequest = new Request("PUT", "/_data_stream/.test-data-stream");
            putRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "product").build());
            Response putResponse = restClient.performRequest(putRequest);
            assertThat(putResponse.getStatusLine().getStatusCode(), is(200));

            // write
            Request index = new Request("POST", "/.test-data-stream/_doc");
            index.setJsonEntity("{ \"@timestamp\": \"2099-03-08T11:06:07.000Z\", \"name\": \"my-name\" }");
            index.addParameter("refresh", "true");

            // no product specified
            ResponseException re = expectThrows(ResponseException.class, () -> restClient.performRequest(index));
            assertThat(re.getMessage(), containsString("reserved for system"));

            // wrong header
            index.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "wrong").build());
            re = expectThrows(ResponseException.class, () -> restClient.performRequest(index));
            assertThat(re.getMessage(), containsString("accessed by product"));

            // correct
            index.setOptions(putRequest.getOptions());
            Response response = restClient.performRequest(index);
            assertEquals(201, response.getStatusLine().getStatusCode());

            Map<String, Object> responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(response.getEntity()),
                false
            );
            String indexName = (String) responseMap.get("_index");
            String id = (String) responseMap.get("_id");

            // get
            Request get = new Request("GET", "/" + indexName + "/_doc/" + id);

            // no product specified
            re = expectThrows(ResponseException.class, () -> restClient.performRequest(get));
            assertThat(re.getMessage(), containsString("reserved for system"));

            // wrong product
            get.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "wrong").build());
            re = expectThrows(ResponseException.class, () -> restClient.performRequest(get));
            assertThat(re.getMessage(), containsString("accessed by product"));

            // correct
            get.setOptions(putRequest.getOptions());
            Response getResponse = restClient.performRequest(get);
            assertThat(getResponse.getStatusLine().getStatusCode(), is(200));

            // search all
            Request search = new Request("GET", "/_search");
            search.addParameter("expand_wildcards", "open,hidden");
            search.setJsonEntity("{ \"query\": { \"match_all\": {} } }");

            // no header
            Response searchResponse = restClient.performRequest(search);
            assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
            responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(searchResponse.getEntity()),
                false
            );
            Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
            List<Object> hitsHits = (List<Object>) hits.get("hits");
            assertThat(hitsHits.size(), is(0));

            // wrong header
            search.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "wrong").build());
            searchResponse = restClient.performRequest(search);
            assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
            responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(searchResponse.getEntity()),
                false
            );
            hits = (Map<String, Object>) responseMap.get("hits");
            hitsHits = (List<Object>) hits.get("hits");
            assertThat(hitsHits.size(), is(0));

            // correct
            search.setOptions(putRequest.getOptions());
            searchResponse = restClient.performRequest(search);
            assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
            responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(searchResponse.getEntity()),
                false
            );
            hits = (Map<String, Object>) responseMap.get("hits");
            hitsHits = (List<Object>) hits.get("hits");
            assertThat(hitsHits.size(), is(1));

            // search the datastream
            Request searchIdx = new Request("GET", "/.test-data-stream/_search");
            searchIdx.setJsonEntity("{ \"query\": { \"match_all\": {} } }");

            // no header
            re = expectThrows(ResponseException.class, () -> restClient.performRequest(searchIdx));
            assertThat(re.getMessage(), containsString("reserved for system"));

            // incorrect header
            searchIdx.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "wrong").build());
            re = expectThrows(ResponseException.class, () -> restClient.performRequest(searchIdx));
            assertThat(re.getMessage(), containsString("accessed by product"));

            // correct
            searchIdx.setOptions(putRequest.getOptions());
            searchResponse = restClient.performRequest(searchIdx);
            assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
            responseMap = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(searchResponse.getEntity()),
                false
            );
            hits = (Map<String, Object>) responseMap.get("hits");
            hitsHits = (List<Object>) hits.get("hits");
            assertThat(hitsHits.size(), is(1));
        }
    }

    @After
    public void cleanup() {
        try {
            PlainActionFuture<ResetFeatureStateStatus> stateStatusPlainActionFuture = new PlainActionFuture<>();
            new TestSystemDataStreamPlugin().cleanUpFeature(
                internalCluster().clusterService(),
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
                            .template(new Template(Settings.EMPTY, mappings, null))
                            .dataStreamTemplate(new DataStreamTemplate())
                            .build(),
                        Map.of(),
                        List.of("product"),
                        "product",
                        ExecutorNames.DEFAULT_SYSTEM_DATA_STREAM_THREAD_POOLS
                    ),
                    new SystemDataStreamDescriptor(
                        ".test-failure-store",
                        "system data stream test with failure store",
                        Type.EXTERNAL,
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(".test-failure-store"))
                            .template(
                                Template.builder()
                                    .mappings(new CompressedXContent("""
                                        {
                                            "properties": {
                                              "@timestamp" : {
                                                "type": "date"
                                              },
                                              "count": {
                                                "type": "long"
                                              }
                                            }
                                        }"""))
                                    .dataStreamOptions(
                                        new DataStreamOptions.Template(DataStreamFailureStore.builder().enabled(true).buildTemplate())
                                    )
                            )
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
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
            return SystemDataStreamIT.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "Integration testing of system data streams";
        }

        @Override
        public void cleanUpFeature(ClusterService clusterService, Client client, ActionListener<ResetFeatureStateStatus> listener) {
            Collection<SystemDataStreamDescriptor> dataStreamDescriptors = getSystemDataStreamDescriptors();
            final DeleteDataStreamAction.Request request = new DeleteDataStreamAction.Request(
                TEST_REQUEST_TIMEOUT,
                dataStreamDescriptors.stream()
                    .map(SystemDataStreamDescriptor::getDataStreamName)
                    .collect(Collectors.toList())
                    .toArray(Strings.EMPTY_ARRAY)
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
                    ActionListener.wrap(response -> SystemIndexPlugin.super.cleanUpFeature(clusterService, client, listener), e -> {
                        Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                        if (unwrapped instanceof ResourceNotFoundException) {
                            SystemIndexPlugin.super.cleanUpFeature(clusterService, client, listener);
                        } else {
                            listener.onFailure(e);
                        }
                    })
                );
            } catch (Exception e) {
                Throwable unwrapped = ExceptionsHelper.unwrapCause(e);
                if (unwrapped instanceof ResourceNotFoundException) {
                    SystemIndexPlugin.super.cleanUpFeature(clusterService, client, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        }
    }
}
