/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests {@code DeprecationLogger} uses the {@code ThreadContext} to add response headers.
 */
public class DeprecationHttpIT extends ESRestTestCase {

    /**
     * Check that configuring deprecation settings causes a warning to be added to the
     * response headers.
     */
    public void testDeprecatedSettingsReturnWarnings() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder()
            .startObject()
            .startObject("transient")
            .field(
                TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1.getKey(),
                !TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1.getDefault(Settings.EMPTY)
            )
            .field(
                TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2.getKey(),
                !TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2.getDefault(Settings.EMPTY)
            )
            // There should be no warning for this field
            .field(
                TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING.getKey(),
                !TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING.getDefault(Settings.EMPTY)
            )
            .endObject()
            .endObject();

        final Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(Strings.toString(builder));
        final Response response = client().performRequest(request);

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        final List<Matcher<String>> headerMatchers = new ArrayList<>(2);

        for (Setting<Boolean> setting : List.of(
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
    private void doTestDeprecationWarningsAppearInHeaders() throws IOException {
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
        request.setEntity(buildSettingsRequest(settings, useDeprecatedField));
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
    }

    /**
     * Check that deprecation messages can be recorded to an index
     */
    public void testDeprecationMessagesCanBeIndexed() throws Exception {
        try {
            configureWriteDeprecationLogsToIndex(true);

            final Request request = new Request("GET", "/_test_cluster/deprecated_settings");
            final RequestOptions options = request.getOptions().toBuilder().addHeader("X-Opaque-Id", "some xid").build();
            request.setOptions(options);
            request.setEntity(
                buildSettingsRequest(Collections.singletonList(TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1), true)
            );
            assertOK(client().performRequest(request));

            assertBusy(() -> {
                Response response;
                try {
                    client().performRequest(new Request("POST", "/.logs-deprecation-elasticsearch/_refresh?ignore_unavailable=true"));
                    response = client().performRequest(new Request("GET", "/.logs-deprecation-elasticsearch/_search"));
                } catch (Exception e) {
                    // It can take a moment for the index to be created. If it doesn't exist then the client
                    // throws an exception. Translate it into an assertion error so that assertBusy() will
                    // continue trying.
                    throw new AssertionError(e);
                }
                assertOK(response);

                ObjectMapper mapper = new ObjectMapper();
                final JsonNode jsonNode = mapper.readTree(response.getEntity().getContent());

                final int hits = jsonNode.at("/hits/total/value").intValue();
                assertThat(hits, greaterThan(0));

                List<Map<String, Object>> documents = new ArrayList<>();

                for (int i = 0; i < hits; i++) {
                    final JsonNode hit = jsonNode.at("/hits/hits/" + i + "/_source");

                    final Map<String, Object> document = new HashMap<>();
                    hit.fields().forEachRemaining(entry -> document.put(entry.getKey(), entry.getValue().textValue()));

                    documents.add(document);
                }

                logger.warn(documents);
                assertThat(documents, hasSize(2));

                assertThat(
                    documents,
                    hasItems(
                        allOf(
                            hasKey("@timestamp"),
                            hasKey("cluster.name"),
                            hasKey("cluster.uuid"),
                            hasKey("log.logger"),
                            hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.namespace", "default"),
                            hasEntry("data_stream.type", "logs"),
                            hasEntry("ecs.version", "1.6"),
                            hasEntry("key", "deprecated_settings"),
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("message", "[deprecated_settings] usage is deprecated. use [settings] instead"),
                            hasKey("node.id"),
                            hasKey("node.name"),
                            hasEntry("x-opaque-id", "some xid"),
                            hasEntry("elasticsearch.event.category", "settings")
                        ),
                        allOf(
                            hasKey("@timestamp"),
                            hasKey("cluster.name"),
                            hasKey("cluster.uuid"),
                            hasKey("log.logger"),
                            hasEntry("data_stream.dataset", "deprecation.elasticsearch"),
                            hasEntry("data_stream.namespace", "default"),
                            hasEntry("data_stream.type", "logs"),
                            hasEntry("ecs.version", "1.6"),
                            hasEntry("key", "deprecated_route"),
                            hasEntry("log.level", "DEPRECATION"),
                            hasEntry("message", "[/_test_cluster/deprecated_settings] exists for deprecated tests"),
                            hasKey("node.id"),
                            hasKey("node.name"),
                            hasEntry("x-opaque-id", "some xid"),
                            hasEntry("elasticsearch.event.category", "api")
                        )
                    )
                );
            }, 30, TimeUnit.SECONDS);
        } finally {
            configureWriteDeprecationLogsToIndex(null);
            client().performRequest(new Request("DELETE", "_data_stream/.logs-deprecation-elasticsearch"));
        }
    }

    private void configureWriteDeprecationLogsToIndex(Boolean value) throws IOException {
        final Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity("{ \"transient\": { \"cluster.deprecation_indexing.enabled\": " + value + " } }");
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

    private HttpEntity buildSettingsRequest(List<Setting<Boolean>> settings, boolean useDeprecatedField) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();

        builder.startObject().startArray(useDeprecatedField ? "deprecated_settings" : "settings");

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
