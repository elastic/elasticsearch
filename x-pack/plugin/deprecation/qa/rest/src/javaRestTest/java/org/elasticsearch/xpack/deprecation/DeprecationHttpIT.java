/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests that deprecation message are returned via response headers, and can be indexed into a data stream.
 */
public class DeprecationHttpIT extends ESRestTestCase {
    // Deprecation log when indexed is using ECS layout
    // which is different than ES JSON used for log files - ES JSON fields are in DeprecatedMessage#KEY_FIELD_NAME
    String KEY_FIELD_NAME = "event.code";
    String X_OPAQUE_ID_FIELD_NAME = "elasticsearch.http.request.x_opaque_id";
    String ELASTIC_ORIGIN_FIELD_NAME = "elasticsearch.elastic_product_origin";

    @Before
    public void assertIndexingIsEnabled() throws Exception {

        // make sure the deprecation logs indexing is enabled
        Response response = client().performRequest(new Request("GET", "/_cluster/settings?include_defaults=true&flat_settings=true"));
        assertOK(response);
        ObjectMapper mapper = new ObjectMapper();
        final JsonNode jsonNode = mapper.readTree(response.getEntity().getContent());

        final boolean defaultValue = jsonNode.at("/defaults/cluster.deprecation_indexing.enabled").asBoolean();
        assertTrue(defaultValue);

        // assert index does not exist, which will prevent previous tests to interfere
        assertBusy(() -> {

            try {
                client().performRequest(new Request("GET", "/_data_stream/" + DeprecationTestUtils.DATA_STREAM_NAME));
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                    return;
                }
            }

            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());
            logger.warn(documents);
            // if data stream is still present, that means that previous test (could be different class) created a deprecation
            // hence resetting again
            resetDeprecationIndexAndCache();
            fail("Index should be removed on startup");
        }, 30, TimeUnit.SECONDS);
    }

    @After
    public void cleanUp() throws Exception {
        resetDeprecationIndexAndCache();

        // switch logging setting to default
        configureWriteDeprecationLogsToIndex(null);
    }

    private void resetDeprecationIndexAndCache() throws Exception {
        // making sure the deprecation indexing cache is reset and index is deleted
        assertBusy(() -> {
            try {
                client().performRequest(new Request("DELETE", "_logging/deprecation_cache"));
                client().performRequest(new Request("DELETE", "/_data_stream/" + DeprecationTestUtils.DATA_STREAM_NAME));
            } catch (Exception e) {
                throw new AssertionError(e);
            }

        }, 30, TimeUnit.SECONDS);
    }

    /**
     * Check that configuring deprecation settings causes a warning to be added to the
     * response headers.
     */
    public void testDeprecatedSettingsReturnWarnings() throws Exception {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder()
                .startObject()
                .startObject("persistent")
                .field(
                    TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1.getKey(),
                    TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1.getDefault(Settings.EMPTY) == false
                )
                .field(
                    TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2.getKey(),
                    TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2.getDefault(Settings.EMPTY) == false
                )
                // There should be no warning for this field
                .field(
                    TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING.getKey(),
                    TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING.getDefault(Settings.EMPTY) == false
                )
                .endObject()
                .endObject();

            final Request request = new Request("PUT", "_cluster/settings");
            ///
            request.setJsonEntity(Strings.toString(builder));
            final Response response = client().performRequest(request);

            final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
            final List<Matcher<String>> headerMatchers = new ArrayList<>(2);

            for (Setting<Boolean> setting : Arrays.asList(
                TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1,
                TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2
            )) {
                headerMatchers.add(
                    equalTo(
                        "["
                            + setting.getKey()
                            + "] setting was deprecated in Elasticsearch and will be removed in a future release! "
                            + "See the breaking changes documentation for the next major version."
                    )
                );
            }

            assertThat(deprecatedWarnings, hasSize(headerMatchers.size()));
            for (final String deprecatedWarning : deprecatedWarnings) {
                assertThat(
                    "Header does not conform to expected pattern",
                    deprecatedWarning,
                    matches(HeaderWarning.WARNING_HEADER_PATTERN.pattern())
                );
            }

            final List<String> actualWarningValues = deprecatedWarnings.stream()
                .map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, true))
                .collect(Collectors.toList());
            for (Matcher<String> headerMatcher : headerMatchers) {
                assertThat(actualWarningValues, hasItem(headerMatcher));
            }

            assertBusy(() -> {
                List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());
                logger.warn(documents);
                assertThat(documents, hasSize(2));
            });

        } finally {
            cleanupSettings();
        }
    }

    private void cleanupSettings() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject("persistent")
            .field(TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1.getKey(), (Boolean) null)
            .field(TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2.getKey(), (Boolean) null)
            // There should be no warning for this field
            .field(TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING.getKey(), (Boolean) null)
            .endObject()
            .endObject();

        final Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(Strings.toString(builder));
        client().performRequest(request);
    }

    /**
     * Attempts to do a scatter/gather request that expects unique responses per sub-request.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/19222")
    public void testUniqueDeprecationResponsesMergedTogether() throws IOException {
        final String[] indices = new String[randomIntBetween(2, 5)];

        // add at least one document for each index
        for (int i = 0; i < indices.length; ++i) {
            indices[i] = "test" + i;

            // create indices with a single shard to reduce noise; the query only deprecates uniquely by index anyway
            createIndex(indices[i], Settings.builder().put("number_of_shards", 1).build());

            int randomDocCount = randomIntBetween(1, 2);

            for (int j = 0; j < randomDocCount; j++) {
                final Request request = new Request("PUT", indices[i] + "/" + j);
                request.setJsonEntity("{ \"field\": " + j + " }");
                assertOK(client().performRequest(request));
            }
        }

        final String commaSeparatedIndices = String.join(",", indices);

        client().performRequest(new Request("POST", commaSeparatedIndices + "/_refresh"));

        // trigger all index deprecations
        Request request = new Request("GET", "/" + commaSeparatedIndices + "/_search");
        request.setJsonEntity("{ \"query\": { \"bool\": { \"filter\": [ { \"deprecated\": {} } ] } } }");
        Response response = client().performRequest(request);
        assertOK(response);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        final List<Matcher<String>> headerMatchers = new ArrayList<>();

        for (String index : indices) {
            headerMatchers.add(containsString(LoggerMessageFormat.format("[{}] index", (Object) index)));
        }

        assertThat(deprecatedWarnings, hasSize(headerMatchers.size()));
        for (Matcher<String> headerMatcher : headerMatchers) {
            assertThat(deprecatedWarnings, hasItem(headerMatcher));
        }
    }

    public void testDeprecationWarningsAppearInHeaders() throws Exception {
        doTestDeprecationWarningsAppearInHeaders();
    }

    public void testDeprecationHeadersDoNotGetStuck() throws Exception {
        doTestDeprecationWarningsAppearInHeaders();
        doTestDeprecationWarningsAppearInHeaders();
        if (rarely()) {
            doTestDeprecationWarningsAppearInHeaders();
        }
    }

    /**
     * Run a request that receives a predictably randomized number of deprecation warnings.
     * <p>
     * Re-running this back-to-back helps to ensure that warnings are not being maintained across requests.
     */
    private void doTestDeprecationWarningsAppearInHeaders() throws Exception {
        final boolean useDeprecatedField = randomBoolean();
        final boolean useNonDeprecatedSetting = randomBoolean();

        // deprecated settings should also trigger a deprecation warning
        final List<Setting<Boolean>> settings = new ArrayList<>(3);
        settings.add(TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1);

        if (randomBoolean()) {
            settings.add(TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2);
        }

        if (useNonDeprecatedSetting) {
            settings.add(TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING);
        }

        Collections.shuffle(settings, random());

        // trigger all deprecations
        Request request = new Request("GET", "/_test_cluster/deprecated_settings");
        request.setEntity(buildSettingsRequest(settings, useDeprecatedField ? "deprecated_settings" : "settings"));
        String xOpaqueId = "XOpaqueId-doTestDeprecationWarningsAppearInHeaders" + randomInt();
        final RequestOptions options = request.getOptions().toBuilder().addHeader("X-Opaque-Id", xOpaqueId).build();
        request.setOptions(options);
        Response response = client().performRequest(request);
        assertOK(response);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        final List<Matcher<String>> headerMatchers = new ArrayList<>(4);

        headerMatchers.add(equalTo(TestDeprecationHeaderRestAction.DEPRECATED_ENDPOINT));
        if (useDeprecatedField) {
            headerMatchers.add(equalTo(TestDeprecationHeaderRestAction.DEPRECATED_USAGE));
        }

        assertThat(deprecatedWarnings, hasSize(headerMatchers.size()));
        for (final String deprecatedWarning : deprecatedWarnings) {
            assertThat(deprecatedWarning, matches(HeaderWarning.WARNING_HEADER_PATTERN.pattern()));
        }
        final List<String> actualWarningValues = deprecatedWarnings.stream()
            .map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, true))
            .collect(Collectors.toList());
        for (Matcher<String> headerMatcher : headerMatchers) {
            assertThat(actualWarningValues, hasItem(headerMatcher));
        }
        // expect to index same number of new deprecations as the number of header warnings in the response
        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());
            long indexedDeprecations = documents.stream().filter(m -> xOpaqueId.equals(m.get(X_OPAQUE_ID_FIELD_NAME))).count();
            assertThat(documents.toString(), indexedDeprecations, equalTo((long) headerMatchers.size()));
        });

    }

    public void testDeprecationRouteThrottling() throws Exception {

        final Request deprecatedRequest = deprecatedRequest("GET", "xOpaqueId-testDeprecationRouteThrottling");
        assertOK(client().performRequest(deprecatedRequest));

        assertOK(client().performRequest(deprecatedRequest));

        final Request postRequest = deprecatedRequest("POST", "xOpaqueId-testDeprecationRouteThrottling");
        assertOK(client().performRequest(postRequest));

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());

            logger.warn(documents);
            assertThat(documents, hasSize(3));

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_POST_/_test_cluster/deprecated_settings"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    ),
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/deprecated_settings"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    ),
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_settings"),
                        hasEntry("message", "[deprecated_settings] usage is deprecated. use [settings] instead")
                    )
                )
            );
        }, 30, TimeUnit.SECONDS);

    }

    public void testDisableDeprecationLogIndexing() throws Exception {
        final Request deprecatedRequest = deprecatedRequest("GET", "xOpaqueId-testDisableDeprecationLogIndexing");
        assertOK(client().performRequest(deprecatedRequest));
        configureWriteDeprecationLogsToIndex(false);

        final Request postRequest = deprecatedRequest("POST", "xOpaqueId-testDisableDeprecationLogIndexing");
        assertOK(client().performRequest(postRequest));

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());

            logger.warn(documents);
            assertThat(documents, hasSize(2));

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/deprecated_settings"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    ),
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_settings"),
                        hasEntry("message", "[deprecated_settings] usage is deprecated. use [settings] instead")
                    )
                )
            );
        }, 30, TimeUnit.SECONDS);

    }

    // triggers two deprecations - endpoint and setting
    private Request deprecatedRequest(String method, String xOpaqueId) throws IOException {
        final Request getRequest = new Request(method, "/_test_cluster/deprecated_settings");
        final RequestOptions options = getRequest.getOptions()
            .toBuilder()
            .addHeader("X-Opaque-Id", xOpaqueId)
            .addHeader("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
            .addHeader("x-elastic-product-origin", "es-test-origin")
            .build();
        getRequest.setOptions(options);
        getRequest.setEntity(
            buildSettingsRequest(
                Collections.singletonList(TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1),
                "deprecated_settings"
            )
        );
        return getRequest;
    }

    /**
     * Check that deprecation messages can be recorded to an index
     */
    public void testDeprecationMessagesCanBeIndexed() throws Exception {

        final Request request = deprecatedRequest("GET", "xOpaqueId-testDeprecationMessagesCanBeIndexed");
        assertOK(client().performRequest(request));

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());

            logger.warn(documents);
            assertThat(documents, hasSize(2));

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, "xOpaqueId-testDeprecationMessagesCanBeIndexed"),
                        hasEntry(Task.TRACE_ID, "0af7651916cd43dd8448eb211c80319c"),
                        hasEntry(ELASTIC_ORIGIN_FIELD_NAME, "es-test-origin"),
                        hasEntry("elasticsearch.event.category", "settings"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasEntry("ecs.version", "1.7"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_settings"),
                        hasEntry("event.dataset", "deprecation.elasticsearch"),
                        hasEntry("log.level", "CRITICAL"),
                        hasKey("log.logger"),
                        hasEntry("message", "[deprecated_settings] usage is deprecated. use [settings] instead")
                    ),
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, "xOpaqueId-testDeprecationMessagesCanBeIndexed"),
                        hasEntry(Task.TRACE_ID, "0af7651916cd43dd8448eb211c80319c"),
                        hasEntry(ELASTIC_ORIGIN_FIELD_NAME, "es-test-origin"),
                        hasEntry("elasticsearch.event.category", "api"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasEntry("ecs.version", "1.7"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/deprecated_settings"),
                        hasEntry("event.dataset", "deprecation.elasticsearch"),
                        hasEntry("log.level", "CRITICAL"),
                        hasKey("log.logger"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    )
                )
            );
        }, 30, TimeUnit.SECONDS);

    }

    /**
     * Check that deprecation messages with WARN level can be recorded to an index
     */
    public void testDeprecationWarnMessagesCanBeIndexed() throws Exception {

        final Request request = new Request("GET", "/_test_cluster/deprecated_settings");
        final RequestOptions options = request.getOptions()
            .toBuilder()
            .addHeader("X-Opaque-Id", "xOpaqueId-testDeprecationWarnMessagesCanBeIndexed")
            .addHeader("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
            .addHeader("x-elastic-product-origin", "es-test-origin")
            .build();
        request.setOptions(options);
        request.setEntity(
            buildSettingsRequest(
                Collections.singletonList(TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1),
                "deprecation_warning"
            )
        );
        assertOK(client().performRequest(request));

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());

            logger.warn(documents);
            assertThat(documents, hasSize(2));

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, "xOpaqueId-testDeprecationWarnMessagesCanBeIndexed"),
                        hasEntry(Task.TRACE_ID, "0af7651916cd43dd8448eb211c80319c"),
                        hasEntry(ELASTIC_ORIGIN_FIELD_NAME, "es-test-origin"),
                        hasEntry("elasticsearch.event.category", "settings"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasEntry("ecs.version", "1.7"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_warn_settings"),
                        hasEntry("event.dataset", "deprecation.elasticsearch"),
                        hasEntry("log.level", "WARN"),
                        hasKey("log.logger"),
                        hasEntry("message", "[deprecated_warn_settings] usage is deprecated but won't be breaking in next version")
                    ),
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, "xOpaqueId-testDeprecationWarnMessagesCanBeIndexed"),
                        hasEntry(Task.TRACE_ID, "0af7651916cd43dd8448eb211c80319c"),
                        hasEntry(ELASTIC_ORIGIN_FIELD_NAME, "es-test-origin"),
                        hasEntry("elasticsearch.event.category", "api"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasEntry("ecs.version", "1.7"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/deprecated_settings"),
                        hasEntry("event.dataset", "deprecation.elasticsearch"),
                        hasEntry("log.level", "CRITICAL"),
                        hasKey("log.logger"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    )
                )
            );
        }, 30, TimeUnit.SECONDS);

    }

    /**
     * Check that deprecation messages can be recorded to an index
     */
    public void testDeprecationIndexingCacheReset() throws Exception {

        final Request deprecatedRequest = deprecatedRequest("GET", "xOpaqueId-testDeprecationIndexingCacheReset");
        assertOK(client().performRequest(deprecatedRequest));

        client().performRequest(new Request("DELETE", "/_logging/deprecation_cache"));

        assertOK(client().performRequest(deprecatedRequest));

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client());

            logger.warn(documents);
            assertThat(documents, hasSize(4));

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/deprecated_settings"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    ),
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/deprecated_settings"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    ),
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_settings"),
                        hasEntry("message", "[deprecated_settings] usage is deprecated. use [settings] instead")
                    ),
                    allOf(
                        hasEntry(KEY_FIELD_NAME, "deprecated_settings"),
                        hasEntry("message", "[deprecated_settings] usage is deprecated. use [settings] instead")
                    )
                )
            );
        }, 30, TimeUnit.SECONDS);

    }

    private void configureWriteDeprecationLogsToIndex(Boolean value) throws IOException {
        final Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity("{ \"persistent\": { \"cluster.deprecation_indexing.enabled\": " + value + " } }");
        final Response response = client().performRequest(request);
        assertOK(response);
    }

    private List<String> getWarningHeaders(Header[] headers) {
        List<String> warnings = new ArrayList<>();

        for (Header header : headers) {
            if (header.getName().equals("Warning")) {
                warnings.add(header.getValue());
            }
        }

        return warnings;
    }

    private HttpEntity buildSettingsRequest(List<Setting<Boolean>> settings, String settingName) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject().startArray(settingName);

        for (Setting<Boolean> setting : settings) {
            builder.value(setting.getKey());
        }

        builder.endArray().endObject();

        return new StringEntity(Strings.toString(builder), ContentType.APPLICATION_JSON);
    }

    /**
     * Builds a REST client that will tolerate warnings in the response headers. The default
     * is to throw an exception.
     */
    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        configureClient(builder, settings);
        builder.setStrictDeprecationMode(false);
        return builder.build();
    }
}
