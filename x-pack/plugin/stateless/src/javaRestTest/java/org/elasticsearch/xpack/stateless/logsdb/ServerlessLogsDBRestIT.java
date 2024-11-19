/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.logsdb;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class ServerlessLogsDBRestIT extends ESRestTestCase {

    private static final String TEST_OPERATOR_USER = "elastic-operator-user";
    private static final String TEST_REGULAR_USER = "elastic-regular-user";
    private static final String TEST_PASSWORD = "elastic-password";
    private static final String CLUSTER_SETTINGS_ENDPOINT = "/_cluster/settings?flat_settings&include_defaults";
    private static final String INDEX_TEMPLATE_ENDPOINT = "/_index_template/";
    private static final String DATA_STREAM_ENDPOINT = "/_data_stream/";
    private static final String ROLLOVER_ENDPOINT = "/_rollover";
    private static final String INDEX_DOCUMENT_ENDPOINT = "/_doc";
    private static final String LOGS_PATTERN = "logs-*-*";

    @ClassRule
    public static final ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .user(TEST_OPERATOR_USER, TEST_PASSWORD)
        .user(TEST_REGULAR_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        return buildAuthSettings(TEST_OPERATOR_USER);
    }

    @Override
    protected Settings restClientSettings() {
        return buildAuthSettings(TEST_REGULAR_USER);
    }

    private Settings buildAuthSettings(final String username) {
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", basicAuthHeaderValue(username, new SecureString(TEST_PASSWORD.toCharArray())))
            .build();
    }

    public void testLogsDBIsEnabledByDefault() throws Exception {
        assertThat(getClusterSettings(adminClient()), containsString("\"cluster.logsdb.enabled\":\"true\""));
    }

    public void testLogsDBDataStream() throws Exception {
        assertOK(putIndexTemplate(client(), randomAlphaOfLength(13).toLowerCase(Locale.ROOT), LOGS_PATTERN, 500));
        final String dataStreamName = randomDataStreamName("logs");
        assertOK(createDataStream(client(), dataStreamName));
        assertOK(indexDocument(client(), dataStreamName, Instant.now().toEpochMilli(), randomFrom("foo", "bar"), randomAlphaOfLength(20)));
        assertOK(rolloverDataStream(client(), dataStreamName, randomBoolean()));
        assertOK(indexDocument(client(), dataStreamName, Instant.now().toEpochMilli(), randomFrom("foo", "bar"), randomAlphaOfLength(20)));

        assertBackingIndexMode(dataStreamName, 0, IndexMode.LOGSDB.name().toLowerCase(Locale.ROOT));
        assertBackingIndexMode(dataStreamName, 1, IndexMode.LOGSDB.name().toLowerCase(Locale.ROOT));
    }

    private Response rolloverDataStream(final RestClient client, final String dataStreamName, boolean lazy) throws IOException {
        return client.performRequest(new Request("POST", dataStreamName + ROLLOVER_ENDPOINT + (lazy ? "?lazy" : "")));
    }

    private static Response indexDocument(
        final RestClient client,
        final String dataStreamName,
        final long timestamp,
        final String hostname,
        final String message
    ) throws IOException {
        final Request request = new Request("POST", dataStreamName + INDEX_DOCUMENT_ENDPOINT);
        request.setJsonEntity(Strings.format("""
            {
                "@timestamp": "%s",
                "host.name": "%s",
                "message": "%s"
            }
            """, DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(timestamp)), hostname, message));

        return client.performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private static void assertBackingIndexMode(final String dataStreamName, int backingIndex, final String expectedIndexMode)
        throws IOException {
        final String indexName = getBackingIndex(client(), dataStreamName, backingIndex);
        final Map<String, Object> settingsByIndex = getIndexSettings(indexName);
        final Map<String, Object> backingIndexSetting = (Map<String, Object>) settingsByIndex.get(indexName);
        final Map<String, Object> indexSettings = (Map<String, Object>) backingIndexSetting.get("settings");
        assertThat(indexSettings.get("index.mode"), Matchers.equalTo(expectedIndexMode));
    }

    private static Response createDataStream(final RestClient client, final String dataStreamName) throws IOException {
        final Request request = new Request("PUT", DATA_STREAM_ENDPOINT + dataStreamName);
        return client.performRequest(request);
    }

    @SuppressWarnings("unchecked")
    private static String getBackingIndex(final RestClient client, final String dataStreamName, int backingIndex) throws IOException {
        final Request request = new Request("GET", DATA_STREAM_ENDPOINT + dataStreamName);
        final List<Object> dataStreams = (List<Object>) entityAsMap(client.performRequest(request)).get("data_streams");
        final Map<String, Object> dataStream = (Map<String, Object>) dataStreams.getFirst();
        final List<Map<String, String>> backingIndices = (List<Map<String, String>>) dataStream.get("indices");
        return backingIndices.get(backingIndex).get("index_name");
    }

    private static String getClusterSettings(final RestClient client) throws IOException {
        return EntityUtils.toString(client.performRequest(new Request("GET", CLUSTER_SETTINGS_ENDPOINT)).getEntity());
    }

    private static Response putIndexTemplate(
        final RestClient client,
        final String templateName,
        final String indexPattern,
        int templatePriority
    ) throws IOException {
        final Request request = new Request("PUT", INDEX_TEMPLATE_ENDPOINT + templateName);
        request.setJsonEntity(Strings.format("""
            {
                 "index_patterns": [ "%s" ],
                 "priority": %d,
                 "data_stream": {}
            }
            """, indexPattern, templatePriority));
        return client.performRequest(request);
    }

    private String randomDataStreamName(final String prefix) {
        return String.format(
            Locale.ROOT,
            "%s-%s-%s",
            prefix,
            randomAlphaOfLength(6).toLowerCase(Locale.ROOT),
            randomAlphaOfLength(6).toLowerCase(Locale.ROOT)
        );
    }
}
