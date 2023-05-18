/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.Version;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptor.Type;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.test.rest.ESRestTestCase.entityAsMap;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public class SystemIndexRestIT extends HttpSmokeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SystemIndexTestPlugin.class);
    }

    public void testSystemIndexAccessBlockedByDefault() throws Exception {
        // create index
        {
            Request putDocRequest = new Request("POST", "/_sys_index_test/add_doc/42");
            Response resp = getRestClient().performRequest(putDocRequest);
            assertThat(resp.getStatusLine().getStatusCode(), equalTo(201));
        }

        // make sure the system index now exists
        assertBusy(() -> {
            Request searchRequest = new Request("GET", "/" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "/_count");
            searchRequest.setOptions(
                expectWarnings(
                    "this request accesses system indices: ["
                        + SystemIndexTestPlugin.SYSTEM_INDEX_NAME
                        + "], but in a future major version, direct access to system indices will be prevented by default"
                )
            );

            // Disallow no indices to cause an exception if the flag above doesn't work
            searchRequest.addParameter("allow_no_indices", "false");
            searchRequest.setJsonEntity("{\"query\": {\"match\":  {\"some_field\":  \"some_value\"}}}");

            final Response searchResponse = getRestClient().performRequest(searchRequest);
            assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
            Map<String, Object> responseMap = entityAsMap(searchResponse);
            assertThat(responseMap, hasKey("count"));
            assertThat(responseMap.get("count"), equalTo(1));
        });

        // And with a partial wildcard
        assertDeprecationWarningOnAccess(".test-*", SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // And with a total wildcard
        assertDeprecationWarningOnAccess(randomFrom("*", "_all"), SystemIndexTestPlugin.SYSTEM_INDEX_NAME);

        // If we're not expanding wildcards, we don't get anything
        {
            Request searchRequest = new Request("GET", "/" + randomFrom("*", "_all") + randomFrom("/_count", "/_search"));
            searchRequest.setJsonEntity("{\"query\": {\"match\":  {\"some_field\":  \"some_value\"}}}");
            searchRequest.addParameter("allow_no_indices", "false");

            ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(searchRequest));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(404));
            assertThat(exception.getMessage(), containsString("no such index"));
        }

        // Try to index a doc directly
        {
            String expectedWarning = "this request accesses system indices: ["
                + SystemIndexTestPlugin.SYSTEM_INDEX_NAME
                + "], but in a "
                + "future major version, direct access to system indices will be prevented by default";
            Request putDocDirectlyRequest = new Request("PUT", "/" + SystemIndexTestPlugin.SYSTEM_INDEX_NAME + "/_doc/43");
            putDocDirectlyRequest.setJsonEntity("{\"some_field\":  \"some_other_value\"}");
            putDocDirectlyRequest.setOptions(expectWarnings(expectedWarning));
            Response response = getRestClient().performRequest(putDocDirectlyRequest);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(201));
        }
    }

    private void assertDeprecationWarningOnAccess(String queryPattern, String warningIndexName) throws IOException {
        String expectedWarning = "this request accesses system indices: ["
            + warningIndexName
            + "], but in a "
            + "future major version, direct access to system indices will be prevented by default";
        Request searchRequest = new Request("GET", "/" + queryPattern + randomFrom("/_count", "/_search"));
        searchRequest.setJsonEntity("{\"query\": {\"match\":  {\"some_field\":  \"some_value\"}}}");
        // Disallow no indices to cause an exception if this resolves to zero indices, so that we're sure it resolved the index
        searchRequest.addParameter("allow_no_indices", "false");
        searchRequest.addParameter("expand_wildcards", "open,hidden");
        searchRequest.setOptions(expectWarnings(expectedWarning));

        Response response = getRestClient().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
    }

    private RequestOptions expectWarnings(String expectedWarning) {
        return RequestOptions.DEFAULT.toBuilder().setWarningsHandler(w -> w.contains(expectedWarning) == false || w.size() != 1).build();
    }

    public static class SystemIndexTestPlugin extends Plugin implements SystemIndexPlugin {

        public static final String SYSTEM_INDEX_NAME = ".test-system-idx";

        public static final Settings SETTINGS = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-1")
            .put(IndexMetadata.SETTING_PRIORITY, Integer.MAX_VALUE)
            .build();

        @Override
        public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster
        ) {
            return List.of(new AddDocRestHandler());
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            try (XContentBuilder builder = jsonBuilder()) {
                builder.startObject();
                {
                    builder.startObject("_meta");
                    builder.field("version", Version.CURRENT.toString());
                    builder.endObject();

                    builder.field("dynamic", "strict");
                    builder.startObject("properties");
                    {
                        builder.startObject("some_field");
                        builder.field("type", "keyword");
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();

                return Collections.singletonList(
                    SystemIndexDescriptor.builder()
                        .setIndexPattern(SYSTEM_INDEX_NAME + "*")
                        .setPrimaryIndex(SYSTEM_INDEX_NAME)
                        .setDescription("Test system index")
                        .setOrigin(getClass().getName())
                        .setVersionMetaKey("version")
                        .setMappings(builder)
                        .setSettings(SETTINGS)
                        .setType(Type.INTERNAL_MANAGED)
                        .build()
                );
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to build " + SYSTEM_INDEX_NAME + " index mappings", e);
            }
        }

        @Override
        public String getFeatureName() {
            return SystemIndexRestIT.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "test plugin";
        }

        public static class AddDocRestHandler extends BaseRestHandler {
            @Override
            public boolean allowSystemIndexAccessByDefault() {
                return true;
            }

            @Override
            public String getName() {
                return "system_index_test_doc_adder";
            }

            @Override
            public List<Route> routes() {
                return List.of(new Route(POST, "/_sys_index_test/add_doc/{id}"));
            }

            @Override
            protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
                IndexRequest indexRequest = new IndexRequest(SYSTEM_INDEX_NAME);
                indexRequest.id(request.param("id"));
                indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                indexRequest.source(Map.of("some_field", "some_value"));
                return channel -> client.index(
                    indexRequest,
                    new RestStatusToXContentListener<>(channel, r -> r.getLocation(indexRequest.routing()))
                );
            }
        }
    }
}
