/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.TelemetryPlugin;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numClientNodes = 1, numDataNodes = 0)
public class RestControllerIT extends ESIntegTestCase {
    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    public void testHeadersEmittedWithChunkedResponses() throws IOException {
        final var client = getRestClient();
        final var response = client.performRequest(new Request("GET", ChunkedResponseWithHeadersPlugin.ROUTE));
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertEquals(ChunkedResponseWithHeadersPlugin.HEADER_VALUE, response.getHeader(ChunkedResponseWithHeadersPlugin.HEADER_NAME));
    }

    public void testMetricsEmittedOnSuccess() throws Exception {
        final var client = getRestClient();
        final var request = new Request("GET", TestEchoStatusCodePlugin.ROUTE);
        request.addParameter("status_code", "200");
        final var response = client.performRequest(request);

        assertEquals(200, response.getStatusLine().getStatusCode());

        assertMeasurement(metric -> {
            assertThat(metric.getLong(), is(1L));
            assertThat(metric.attributes(), hasEntry(RestController.HANDLER_NAME_KEY, TestEchoStatusCodePlugin.NAME));
            assertThat(metric.attributes(), hasEntry(RestController.REQUEST_METHOD_KEY, "GET"));
            assertThat(metric.attributes(), hasEntry(RestController.STATUS_CODE_KEY, 200));
        });
    }

    public void testMetricsEmittedOnRestError() throws Exception {
        final var client = getRestClient();
        final var request = new Request("GET", TestEchoStatusCodePlugin.ROUTE);
        request.addParameter("status_code", "503");
        final var response = expectThrows(ResponseException.class, () -> client.performRequest(request));

        assertEquals(503, response.getResponse().getStatusLine().getStatusCode());
        assertMeasurement(metric -> {
            assertThat(metric.getLong(), is(1L));
            assertThat(metric.attributes(), hasEntry(RestController.HANDLER_NAME_KEY, TestEchoStatusCodePlugin.NAME));
            assertThat(metric.attributes(), hasEntry(RestController.REQUEST_METHOD_KEY, "GET"));
            assertThat(metric.attributes(), hasEntry(RestController.STATUS_CODE_KEY, 503));
        });
    }

    public void testMetricsEmittedOnWrongMethod() throws Exception {
        final var client = getRestClient();
        final var request = new Request("DELETE", TestEchoStatusCodePlugin.ROUTE);
        final var response = expectThrows(ResponseException.class, () -> client.performRequest(request));

        assertEquals(405, response.getResponse().getStatusLine().getStatusCode());
        assertMeasurement(metric -> {
            assertThat(metric.getLong(), is(1L));
            assertThat(metric.attributes(), hasEntry(RestController.STATUS_CODE_KEY, RestStatus.METHOD_NOT_ALLOWED.getStatus()));
        });
    }

    private void assertMeasurement(Consumer<Measurement> measurementConsumer) throws Exception {
        assertBusy(() -> {
            var measurements = new ArrayList<Measurement>();
            for (var nodeName : internalCluster().getNodeNames()) {
                PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, nodeName);
                var telemetryPlugins = pluginsService.filterPlugins(TelemetryPlugin.class).toList();

                assertThat(telemetryPlugins, hasSize(1));
                assertThat(telemetryPlugins.get(0), instanceOf(TestTelemetryPlugin.class));
                var telemetryPlugin = (TestTelemetryPlugin) telemetryPlugins.get(0);

                telemetryPlugin.collect();

                final var metrics = telemetryPlugin.getLongCounterMeasurement(RestController.METRIC_REQUESTS_TOTAL);
                logger.info("collecting [{}] metrics from [{}]", metrics.size(), nodeName);
                measurements.addAll(metrics);
            }
            assertThat(measurements, hasSize(1));
            measurementConsumer.accept(measurements.get(0));
        });
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ChunkedResponseWithHeadersPlugin.class, TestEchoStatusCodePlugin.class, TestTelemetryPlugin.class);
    }

    public static class TestEchoStatusCodePlugin extends Plugin implements ActionPlugin {
        static final String ROUTE = "/_test/echo_status_code";
        static final String NAME = "test_echo_status_code";

        private static final Logger logger = LogManager.getLogger(TestEchoStatusCodePlugin.class);

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return NAME;
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, ROUTE), new Route(RestRequest.Method.POST, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    var statusCode = request.param("status_code");
                    logger.info("received echo request for {}", statusCode);

                    var restStatus = RestStatus.fromCode(Integer.parseInt(statusCode));
                    return channel -> {
                        final var response = RestResponse.chunked(
                            restStatus,
                            ChunkedRestResponseBodyPart.fromXContent(
                                params -> Iterators.single((b, p) -> b.startObject().endObject()),
                                request,
                                channel
                            ),
                            null
                        );
                        channel.sendResponse(response);
                        logger.info("sent response");
                    };
                }
            });
        }
    }

    public static class ChunkedResponseWithHeadersPlugin extends Plugin implements ActionPlugin {

        static final String ROUTE = "/_test/chunked_response_with_headers";
        static final String HEADER_NAME = "test-header";
        static final String HEADER_VALUE = "test-header-value";

        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return ChunkedResponseWithHeadersPlugin.class.getCanonicalName();
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, ROUTE));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    return channel -> {
                        final var response = RestResponse.chunked(
                            RestStatus.OK,
                            ChunkedRestResponseBodyPart.fromXContent(
                                params -> Iterators.single((b, p) -> b.startObject().endObject()),
                                request,
                                channel
                            ),
                            null
                        );
                        response.addHeader(HEADER_NAME, HEADER_VALUE);
                        channel.sendResponse(response);
                    };
                }
            });
        }
    }
}
