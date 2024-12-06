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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.logging.DeprecatedMessage.KEY_FIELD_NAME;
import static org.elasticsearch.common.logging.DeprecatedMessage.X_OPAQUE_ID_FIELD_NAME;
import static org.elasticsearch.xpack.deprecation.TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1;
import static org.elasticsearch.xpack.deprecation.TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2;
import static org.elasticsearch.xpack.deprecation.TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;

/**
 * Tests that deprecation message are returned via response headers, and can be indexed into a data stream.
 */
public class DeprecationHttpIT extends ESRestTestCase {

    @Rule
    public TestName testName = new TestName();

    private String xOpaqueId() {
        String name = testName.getMethodName();
        int pos = name.indexOf(" "); // additional suffix in case of repeated runs
        return pos == -1 ? name : name.substring(0, pos) + "-" + name.hashCode();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true; // isolation is based on xOpaqueId
    }

    @Before
    public void assertIndexingIsEnabled() throws Exception {
        // make sure the deprecation logs indexing is enabled
        Response response = performScopedRequest(new Request("GET", "/_cluster/settings?include_defaults=true&flat_settings=true"));

        ObjectMapper mapper = new ObjectMapper();
        final JsonNode jsonNode = mapper.readTree(response.getEntity().getContent());
        final boolean defaultValue = jsonNode.at("/defaults/cluster.deprecation_indexing.enabled").asBoolean();
        assertTrue(defaultValue);
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
                .field(TEST_DEPRECATED_SETTING_TRUE1.getKey(), TEST_DEPRECATED_SETTING_TRUE1.getDefault(Settings.EMPTY) == false)
                .field(TEST_DEPRECATED_SETTING_TRUE2.getKey(), TEST_DEPRECATED_SETTING_TRUE2.getDefault(Settings.EMPTY) == false)
                // There should be no warning for this field
                .field(TEST_NOT_DEPRECATED_SETTING.getKey(), TEST_NOT_DEPRECATED_SETTING.getDefault(Settings.EMPTY) == false)
                .endObject()
                .endObject();

            final Request request = new Request("PUT", "_cluster/settings");
            request.setJsonEntity(Strings.toString(builder));
            final Response response = performScopedRequest(request);

            final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
            assertThat(deprecatedWarnings, everyItem(matchesRegex(HeaderWarning.WARNING_HEADER_PATTERN)));
            assertThat(
                extractWarningValuesFromWarningHeaders(deprecatedWarnings),
                containsInAnyOrder(
                    matchDeprecationWarning(TEST_DEPRECATED_SETTING_TRUE1),
                    matchDeprecationWarning(TEST_DEPRECATED_SETTING_TRUE2)
                )
            );

            assertBusy(() -> {
                List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());
                logger.warn(documents);
                assertThat(documents, hasSize(2));
            }, 45, TimeUnit.SECONDS);
        } finally {
            Response response = cleanupSettings();
            List<String> warningHeaders = getWarningHeaders(response.getHeaders());
            logger.warn("Warning headers on cleanup: {}", warningHeaders);
        }
    }

    private Matcher<String> matchDeprecationWarning(Setting<?> setting) {
        var format = "[%s] setting was deprecated in Elasticsearch and will be removed in a future release.";
        return equalTo(Strings.format(format, setting.getKey()));
    }

    private Response cleanupSettings() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject("persistent")
            .field(TEST_DEPRECATED_SETTING_TRUE1.getKey(), (Boolean) null)
            .field(TEST_DEPRECATED_SETTING_TRUE2.getKey(), (Boolean) null)
            // There should be no warning for this field
            .field(TEST_NOT_DEPRECATED_SETTING.getKey(), (Boolean) null)
            .endObject()
            .endObject();

        final Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(Strings.toString(builder));
        return performScopedRequest(request, xOpaqueId() + "-cleanup");
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
                performScopedRequest(request);
            }
        }

        final String commaSeparatedIndices = String.join(",", indices);

        performScopedRequest(new Request("POST", commaSeparatedIndices + "/_refresh"));
        // trigger all index deprecations
        Request request = new Request("GET", "/" + commaSeparatedIndices + "/_search");
        request.setJsonEntity("{ \"query\": { \"bool\": { \"filter\": [ { \"deprecated\": {} } ] } } }");
        Response response = performScopedRequest(request);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        final List<Matcher<? super String>> headerMatchers = new ArrayList<>();

        for (String index : indices) {
            headerMatchers.add(containsString(LoggerMessageFormat.format("[{}] index", (Object) index)));
        }

        assertThat(deprecatedWarnings, containsInAnyOrder(headerMatchers));
    }

    public void testDeprecationWarningsAppearInHeaders() throws Exception {
        doTestDeprecationWarningsAppearInHeaders(xOpaqueId());
    }

    public void testDeprecationHeadersDoNotGetStuck() throws Exception {
        for (int i = 0; i < 3; i++) {
            doTestDeprecationWarningsAppearInHeaders(xOpaqueId() + "-" + i);
        }
    }

    /**
     * Run a request that receives a predictably randomized number of deprecation warnings.
     * <p>
     * Re-running this back-to-back helps to ensure that warnings are not being maintained across requests.
     */
    private void doTestDeprecationWarningsAppearInHeaders(String xOpaqueId) throws Exception {
        final boolean useDeprecatedField = randomBoolean();
        final boolean useNonDeprecatedSetting = randomBoolean();

        // deprecated settings should also trigger a deprecation warning
        final List<Setting<Boolean>> settings = new ArrayList<>(3);
        settings.add(TEST_DEPRECATED_SETTING_TRUE1);

        if (randomBoolean()) {
            settings.add(TEST_DEPRECATED_SETTING_TRUE2);
        }

        if (useNonDeprecatedSetting) {
            settings.add(TEST_NOT_DEPRECATED_SETTING);
        }

        Collections.shuffle(settings, random());

        // trigger all deprecations
        Request request = new Request("GET", "/_test_cluster/deprecated_settings");
        request.setEntity(buildSettingsRequest(settings, useDeprecatedField ? "deprecated_settings" : "settings"));
        Response response = performScopedRequest(request, xOpaqueId);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        final List<Matcher<? super String>> headerMatchers = new ArrayList<>(4);

        headerMatchers.add(equalTo(TestDeprecationHeaderRestAction.DEPRECATED_ENDPOINT));
        if (useDeprecatedField) {
            headerMatchers.add(equalTo(TestDeprecationHeaderRestAction.DEPRECATED_USAGE));
        }

        assertThat(deprecatedWarnings, everyItem(matchesRegex(HeaderWarning.WARNING_HEADER_PATTERN)));
        assertThat(extractWarningValuesFromWarningHeaders(deprecatedWarnings), containsInAnyOrder(headerMatchers));

        // expect to index same number of new deprecations as the number of header warnings in the response
        assertBusy(() -> {
            var documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId);
            logger.warn(documents);
            assertThat(documents, hasSize(headerMatchers.size()));
        }, 45, TimeUnit.SECONDS);
    }

    public void testDeprecationRouteThrottling() throws Exception {
        performScopedRequest(deprecatedRequest("GET"));
        performScopedRequest(deprecatedRequest("GET"));
        performScopedRequest(deprecatedRequest("POST"));

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

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
        }, 45, TimeUnit.SECONDS);

    }

    public void testDisableDeprecationLogIndexing() throws Exception {
        performScopedRequest(deprecatedRequest("GET"));
        configureWriteDeprecationLogsToIndex(false);

        try {
            performScopedRequest(deprecatedRequest("POST"));
            assertBusy(() -> {
                List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

                logger.warn(documents);

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
            }, 45, TimeUnit.SECONDS);
        } finally {
            configureWriteDeprecationLogsToIndex(null);
        }

    }

    // triggers two deprecations - endpoint and setting
    private Request deprecatedRequest(String method) throws IOException {
        final Request getRequest = new Request(method, "/_test_cluster/deprecated_settings");
        getRequest.setEntity(buildSettingsRequest(Collections.singletonList(TEST_DEPRECATED_SETTING_TRUE1), "deprecated_settings"));
        return getRequest;
    }

    /**
     * Check that deprecation messages can be recorded to an index
     */
    public void testDeprecationMessagesCanBeIndexed() throws Exception {
        performScopedRequest(deprecatedRequest("GET"));

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, xOpaqueId()),
                        hasEntry("elasticsearch.event.category", "settings"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "elasticsearch.deprecation"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasKey("ecs.version"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_settings"),
                        hasEntry("event.dataset", "elasticsearch.deprecation"),
                        hasEntry("log.level", "WARN"),
                        hasKey("log.logger"),
                        hasEntry("message", "[deprecated_settings] usage is deprecated. use [settings] instead")
                    ),
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, xOpaqueId()),
                        hasEntry("elasticsearch.event.category", "api"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "elasticsearch.deprecation"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasKey("ecs.version"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/deprecated_settings"),
                        hasEntry("event.dataset", "elasticsearch.deprecation"),
                        hasEntry("log.level", "WARN"),
                        hasKey("log.logger"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    )
                )
            );
        }, 45, TimeUnit.SECONDS);

    }

    /**
     * Check that a deprecation message with CRITICAL level can be recorded to an index
     */
    public void testDeprecationCriticalWarnMessagesCanBeIndexed() throws Exception {
        final Request request = new Request("GET", "/_test_cluster/only_deprecated_setting");
        request.setEntity(
            buildSettingsRequest(
                Collections.singletonList(TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE3),
                "deprecation_critical"
            )
        );
        performScopedRequest(request);

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, xOpaqueId()),
                        hasEntry("elasticsearch.event.category", "settings"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "elasticsearch.deprecation"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasKey("ecs.version"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_critical_settings"),
                        hasEntry("event.dataset", "elasticsearch.deprecation"),
                        hasEntry("log.level", "CRITICAL"),
                        hasKey("log.logger"),
                        hasEntry("message", "[deprecated_settings] usage is deprecated. use [settings] instead")
                    )
                )
            );
        }, 45, TimeUnit.SECONDS);

    }

    /**
     * Check that deprecation messages with WARN level can be recorded to an index
     */
    public void testDeprecationWarnMessagesCanBeIndexed() throws Exception {

        final Request request = new Request("GET", "/_test_cluster/deprecated_settings");
        request.setEntity(buildSettingsRequest(Collections.singletonList(TEST_DEPRECATED_SETTING_TRUE1), "deprecation_warning"));
        performScopedRequest(request);

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, xOpaqueId()),
                        hasEntry("elasticsearch.event.category", "settings"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "elasticsearch.deprecation"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasKey("ecs.version"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_warn_settings"),
                        hasEntry("event.dataset", "elasticsearch.deprecation"),
                        hasEntry("log.level", "WARN"),
                        hasKey("log.logger"),
                        hasEntry("message", "[deprecated_warn_settings] usage is deprecated but won't be breaking in next version")
                    ),
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, xOpaqueId()),
                        hasEntry("elasticsearch.event.category", "api"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "elasticsearch.deprecation"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasKey("ecs.version"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/deprecated_settings"),
                        hasEntry("event.dataset", "elasticsearch.deprecation"),
                        hasEntry("log.level", "WARN"),
                        hasKey("log.logger"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    )
                )
            );
        }, 45, TimeUnit.SECONDS);

    }

    public void testDeprecateAndKeep() throws Exception {
        final Request request = new Request("GET", "/_test_cluster/deprecated_but_dont_remove");
        request.setEntity(buildSettingsRequest(Collections.singletonList(TEST_NOT_DEPRECATED_SETTING), "settings"));
        Response response = performScopedRequest(request);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        assertThat(
            extractWarningValuesFromWarningHeaders(deprecatedWarnings),
            containsInAnyOrder("[/_test_cluster/deprecated_but_dont_remove] is deprecated, but no plans to remove quite yet")
        );

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

            // only assert the relevant fields: level, message, and category
            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasEntry("elasticsearch.event.category", "api"),
                        hasEntry("log.level", "WARN"),
                        hasEntry("message", "[/_test_cluster/deprecated_but_dont_remove] is deprecated, but no plans to remove quite yet")
                    )
                )
            );
        }, 45, TimeUnit.SECONDS);
    }

    public void testReplacesInCurrentVersion() throws Exception {
        final Request request = new Request("GET", "/_test_cluster/old_name1"); // deprecated in current version
        request.setEntity(buildSettingsRequest(Collections.singletonList(TEST_NOT_DEPRECATED_SETTING), "settings"));
        Response response = performScopedRequest(request);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        assertThat(
            extractWarningValuesFromWarningHeaders(deprecatedWarnings),
            containsInAnyOrder("[GET /_test_cluster/old_name1] is deprecated! Use [GET /_test_cluster/new_name1] instead.")
        );

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

            // only assert the relevant fields: level, message, and category
            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasEntry("elasticsearch.event.category", "api"),
                        hasEntry("log.level", "WARN"),
                        hasEntry("message", "[GET /_test_cluster/old_name1] is deprecated! Use [GET /_test_cluster/new_name1] instead.")
                    )
                )
            );
        }, 45, TimeUnit.SECONDS);
    }

    public void testReplacesInCompatibleVersion() throws Exception {
        final Request request = new Request("GET", "/_test_cluster/old_name2"); // deprecated in minimum supported version
        request.setEntity(buildSettingsRequest(Collections.singletonList(TEST_DEPRECATED_SETTING_TRUE1), "deprecated_settings"));
        final RequestOptions compatibleOptions = request.getOptions()
            .toBuilder()
            .addHeader("Accept", "application/vnd.elasticsearch+json;compatible-with=" + RestApiVersion.minimumSupported().major)
            .addHeader("Content-Type", "application/vnd.elasticsearch+json;compatible-with=" + RestApiVersion.minimumSupported().major)
            .build();
        request.setOptions(compatibleOptions);
        Response response = performScopedRequest(request);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        assertThat(
            extractWarningValuesFromWarningHeaders(deprecatedWarnings),
            containsInAnyOrder(
                "[GET /_test_cluster/old_name2] is deprecated! Use [GET /_test_cluster/new_name2] instead.",
                "You are using a compatible API for this request"
            )
        );
        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

            // only assert the relevant fields: level, message, and category
            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(

                        hasEntry("elasticsearch.event.category", "compatible_api"),
                        hasEntry("log.level", "CRITICAL"),
                        hasEntry("message", "[GET /_test_cluster/old_name2] is deprecated! Use [GET /_test_cluster/new_name2] instead.")
                    ),
                    allOf(
                        hasEntry("elasticsearch.event.category", "compatible_api"),
                        hasEntry("log.level", "CRITICAL"),
                        // this message comes from the test, not production code. this is the message for setting the deprecated setting
                        hasEntry("message", "You are using a compatible API for this request")
                    )
                )
            );
        }, 45, TimeUnit.SECONDS);
    }

    /**
     * Check that log messages about REST API compatibility are recorded to an index
     */
    public void testCompatibleMessagesCanBeIndexed() throws Exception {

        final Request compatibleRequest = new Request("GET", "/_test_cluster/compat_only");
        final RequestOptions compatibleOptions = compatibleRequest.getOptions()
            .toBuilder()
            .addHeader("Accept", "application/vnd.elasticsearch+json;compatible-with=" + RestApiVersion.minimumSupported().major)
            .addHeader("Content-Type", "application/vnd.elasticsearch+json;compatible-with=" + RestApiVersion.minimumSupported().major)
            .build();
        compatibleRequest.setOptions(compatibleOptions);
        compatibleRequest.setEntity(buildSettingsRequest(Collections.singletonList(TEST_DEPRECATED_SETTING_TRUE1), "deprecated_settings"));
        Response deprecatedApiResponse = performScopedRequest(compatibleRequest);

        final List<String> deprecatedWarnings = getWarningHeaders(deprecatedApiResponse.getHeaders());
        assertThat(
            extractWarningValuesFromWarningHeaders(deprecatedWarnings),
            containsInAnyOrder(TestDeprecationHeaderRestAction.DEPRECATED_ENDPOINT, TestDeprecationHeaderRestAction.COMPATIBLE_API_USAGE)
        );

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

            assertThat(
                documents,
                containsInAnyOrder(
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, xOpaqueId()),
                        hasEntry("elasticsearch.event.category", "compatible_api"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "elasticsearch.deprecation"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasKey("ecs.version"),
                        hasEntry(KEY_FIELD_NAME, "compatible_key"),
                        hasEntry("event.dataset", "elasticsearch.deprecation"),
                        hasEntry("log.level", "CRITICAL"),
                        hasKey("log.logger"),
                        hasEntry("message", "You are using a compatible API for this request")
                    ),
                    allOf(
                        hasKey("@timestamp"),
                        hasKey("elasticsearch.cluster.name"),
                        hasKey("elasticsearch.cluster.uuid"),
                        hasEntry(X_OPAQUE_ID_FIELD_NAME, xOpaqueId()),
                        hasEntry("elasticsearch.event.category", "compatible_api"),
                        hasKey("elasticsearch.node.id"),
                        hasKey("elasticsearch.node.name"),
                        hasEntry("data_stream.dataset", "elasticsearch.deprecation"),
                        hasEntry("data_stream.namespace", "default"),
                        hasEntry("data_stream.type", "logs"),
                        hasKey("ecs.version"),
                        hasEntry(KEY_FIELD_NAME, "deprecated_route_GET_/_test_cluster/compat_only"),
                        hasEntry("event.dataset", "elasticsearch.deprecation"),
                        hasEntry("log.level", "CRITICAL"),
                        hasKey("log.logger"),
                        hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests")
                    )
                )
            );
        }, 45, TimeUnit.SECONDS);

    }

    /**
     * Check that deprecation messages can be recorded to an index
     */
    public void testDeprecationIndexingCacheReset() throws Exception {

        performScopedRequest(deprecatedRequest("GET"));

        performScopedRequest(new Request("DELETE", "/_logging/deprecation_cache"));

        performScopedRequest(deprecatedRequest("GET"));

        assertBusy(() -> {
            List<Map<String, Object>> documents = DeprecationTestUtils.getIndexedDeprecations(client(), xOpaqueId());

            logger.warn(documents);

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
        }, 45, TimeUnit.SECONDS);

    }

    private void configureWriteDeprecationLogsToIndex(Boolean value) throws IOException {
        final Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity("{ \"persistent\": { \"cluster.deprecation_indexing.enabled\": " + value + " } }");
        performScopedRequest(request);
    }

    private List<String> getWarningHeaders(Header[] headers) {
        return Arrays.stream(headers).filter(h -> h.getName().equals("Warning")).map(Header::getValue).toList();
    }

    private List<String> extractWarningValuesFromWarningHeaders(List<String> deprecatedWarnings) {
        return deprecatedWarnings.stream()
            .map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, true))
            .collect(Collectors.toList());
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

    private Response performScopedRequest(Request req) throws IOException {
        return performScopedRequest(req, xOpaqueId());
    }

    private Response performScopedRequest(Request req, String xOpaqueId) throws IOException {
        req.setOptions(req.getOptions().toBuilder().addHeader("X-Opaque-Id", xOpaqueId).build());
        Response response = client().performRequest(req);
        assertOK(response);
        return response;
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
