/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.test.hamcrest.RegexMatcher.matches;
import static org.elasticsearch.xpack.deprecation.TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE1;
import static org.elasticsearch.xpack.deprecation.TestDeprecationHeaderRestAction.TEST_DEPRECATED_SETTING_TRUE2;
import static org.elasticsearch.xpack.deprecation.TestDeprecationHeaderRestAction.TEST_NOT_DEPRECATED_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests {@code DeprecationLogger} uses the {@code ThreadContext} to add response headers.
 */
public class DeprecationHttpIT extends ESSingleNodeTestCase {

    private static RestClient restClient;

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(Netty4Plugin.class, XPackPlugin.class, Deprecation.class, TestDeprecationPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            // change values of deprecated settings so that accessing them is logged
            .put(TEST_DEPRECATED_SETTING_TRUE1.getKey(), ! TEST_DEPRECATED_SETTING_TRUE1.getDefault(Settings.EMPTY))
            .put(TEST_DEPRECATED_SETTING_TRUE2.getKey(), ! TEST_DEPRECATED_SETTING_TRUE2.getDefault(Settings.EMPTY))
            // non-deprecated setting to ensure not everything is logged
            .put(TEST_NOT_DEPRECATED_SETTING.getKey(), ! TEST_NOT_DEPRECATED_SETTING.getDefault(Settings.EMPTY))
            .build();
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
            assertTrue(
                client().admin()
                    .indices()
                    .prepareCreate(indices[i])
                    .setSettings(Settings.builder().put("number_of_shards", 1))
                    .get()
                    .isAcknowledged()
            );

            int randomDocCount = randomIntBetween(1, 2);

            for (int j = 0; j < randomDocCount; ++j) {
                client().prepareIndex(indices[i])
                    .setId(Integer.toString(j))
                    .setSource("{\"field\":" + j + "}", XContentType.JSON)
                    .execute()
                    .actionGet();
            }
        }

        client().admin().indices().refresh(new RefreshRequest(indices));

        final String commaSeparatedIndices = String.join(",", indices);

        // trigger all index deprecations
        Request request = new Request("GET", "/" + commaSeparatedIndices + "/_search");
        request.setJsonEntity("{\"query\":{\"bool\":{\"filter\":[{\"" + TestDeprecatedQueryBuilder.NAME +  "\":{}}]}}}");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        final List<Matcher<String>> headerMatchers = new ArrayList<>(indices.length);

        for (String index : indices) {
            headerMatchers.add(containsString(LoggerMessageFormat.format("[{}] index", (Object)index)));
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
        request.setEntity(buildSettingsRequest(settings, useDeprecatedField));
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(OK.getStatus()));

        final List<String> deprecatedWarnings = getWarningHeaders(response.getHeaders());
        final List<Matcher<String>> headerMatchers = new ArrayList<>(4);

        headerMatchers.add(equalTo(TestDeprecationHeaderRestAction.DEPRECATED_ENDPOINT));
        if (useDeprecatedField) {
            headerMatchers.add(equalTo(TestDeprecationHeaderRestAction.DEPRECATED_USAGE));
        }
        for (Setting<?> setting : settings) {
            if (setting.isDeprecated()) {
                headerMatchers.add(equalTo(
                        "[" + setting.getKey() + "] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                        "See the breaking changes documentation for the next major version."));
            }
        }

        assertThat(deprecatedWarnings, hasSize(headerMatchers.size()));
        for (final String deprecatedWarning : deprecatedWarnings) {
            assertThat(deprecatedWarning, matches(HeaderWarning.WARNING_HEADER_PATTERN.pattern()));
        }
        final List<String> actualWarningValues =
                deprecatedWarnings.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, true))
                    .collect(Collectors.toList());
        for (Matcher<String> headerMatcher : headerMatchers) {
            assertThat(actualWarningValues, hasItem(headerMatcher));
        }
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

    protected RestClient getRestClient() {
        return getRestClient(client());
    }

    private static synchronized RestClient getRestClient(Client client) {
        if (restClient == null) {
            restClient = buildRestClient(client);
        }
        return restClient;
    }

    private static RestClient buildRestClient(Client client ) {
        NodesInfoResponse nodesInfoResponse = client.admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        assertThat(nodesInfoResponse.getNodes(), hasSize(1));

        NodeInfo node = nodesInfoResponse.getNodes().get(0);
        assertNotNull(node.getInfo(HttpInfo.class));

        TransportAddress publishAddress = node.getInfo(HttpInfo.class).address().publishAddress();
        InetSocketAddress address = publishAddress.address();
        final HttpHost host = new HttpHost(NetworkAddress.format(address.getAddress()), address.getPort(), "http");
        RestClientBuilder builder = RestClient.builder(host);
        return builder.build();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (restClient != null) {
            IOUtils.closeWhileHandlingException(restClient);
            restClient = null;
        }
    }
}
