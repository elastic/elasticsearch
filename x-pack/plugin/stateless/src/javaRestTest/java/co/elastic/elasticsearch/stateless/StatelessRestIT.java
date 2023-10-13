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

package co.elastic.elasticsearch.stateless;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

public class StatelessRestIT extends ESRestTestCase {

    private static final String TEST_OPERATOR = "elastic-operator";
    private static final String TEST_PASSWORD = "elastic-password";
    private static final String TEST_USER = "elastic-user";

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .user(TEST_OPERATOR, TEST_PASSWORD)
        .user(TEST_USER, TEST_PASSWORD, "_es_test_root", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_OPERATOR, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(TEST_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testDefaultNumberOfShardsForRegularIndices() throws Exception {
        // regular index defaults to 3 shards in stateless
        final int defaultNumberOfShardsForRegularIndices = 3;
        final String indexName = randomIdentifier();
        assertOK(client().performRequest(new Request("PUT", indexName)));
        assertNumberOfShards(indexName, defaultNumberOfShardsForRegularIndices);

        // default can be overridden per index (requires operator level access)
        final String indexNameWithPerIndexOverride = randomIdentifier();
        final Request putIndexRequest = new Request("PUT", indexNameWithPerIndexOverride);
        final int numberOfShardsPerIndexOverride = randomValueOtherThan(
            defaultNumberOfShardsForRegularIndices,
            () -> randomIntBetween(1, 10)
        );
        putIndexRequest.setJsonEntity(Strings.format("""
            {
              "settings": {
                "number_of_shards": %s
              }
            }""", numberOfShardsPerIndexOverride));
        assertOK(adminClient().performRequest(putIndexRequest));
        assertNumberOfShards(indexNameWithPerIndexOverride, numberOfShardsPerIndexOverride);

        // Change the default cluster wide (requires operator level access)
        final int numberOfShardsNewDefault = randomlyChangeDefaultNumberOfShardsClusterWide();

        final String anotherIndexName = randomIdentifier();
        assertOK(client().performRequest(new Request("PUT", anotherIndexName)));
        assertNumberOfShards(anotherIndexName, numberOfShardsNewDefault);

        // All previously created indices are *not* affected by the new cluster wide default
        assertNumberOfShards(indexName, defaultNumberOfShardsForRegularIndices);
        assertNumberOfShards(indexNameWithPerIndexOverride, numberOfShardsPerIndexOverride);
    }

    public void testDefaultNumberOfShardsNotChangedForDataStream() throws IOException {
        // data stream defaults to 1
        final int defaultNumberOfShardsForDataStreamIndices = 1;
        final String dataStreamName = "test-data-stream";
        final Request putIndexTemplateRequest = new Request("PUT", "_index_template/test-data-stream");
        putIndexTemplateRequest.setJsonEntity("""
            {
              "index_patterns" : ["test-data-stream"],
              "data_stream": {},
              "template": {
                "settings" : {
                }
              }
            }""");
        assertOK(client().performRequest(putIndexTemplateRequest));
        assertOK(client().performRequest(new Request("PUT", "_data_stream/" + dataStreamName)));
        final ObjectPath getDataStream = assertOKAndCreateObjectPath(
            client().performRequest(new Request("GET", "_data_stream/" + dataStreamName))
        );
        final String dataStreamIndexName = getDataStream.evaluate("data_streams.0.indices.0.index_name");
        assertNumberOfShards(dataStreamIndexName, defaultNumberOfShardsForDataStreamIndices);

        // Change cluster wide default has no impact
        randomlyChangeDefaultNumberOfShardsClusterWide();
        assertNumberOfShards(dataStreamIndexName, defaultNumberOfShardsForDataStreamIndices);
    }

    public void testDefaultNumberOfShardsNotChangedForSystemIndices() throws IOException {
        // system indices are _not_ affected by stateless default, i.e. they will still have 1 primary shard if
        // not explicitly specified otherwise
        final int defaultNumberOfShardsForSystemIndices = 1;
        // Pick some system indices to create. Not all of them have explicit number_of_shards setting.
        // Some are aliases while others are concrete index names
        final List<String> systemIndices = List.of(
            randomFrom(".kibana", ".kibana_8.10"),
            ".reporting-7",
            randomFrom(".fleet-enrollment-api-keys", ".fleet-enrollment-api-keys-7"),
            randomFrom(".security", ".security-7")
        );
        for (String systemIndexName : systemIndices) {
            final Request putSystemIndex = new Request("PUT", systemIndexName);
            assertOK(client().performRequest(putSystemIndex));
            assertNumberOfShards(
                systemIndexName,
                defaultNumberOfShardsForSystemIndices,
                RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE)
            );
        }

        // System data stream is not affected by stateless default
        final int defaultNumberOfShardsForSystemDataStream = 1;
        final String systemDataStreamName = ".fleet-actions-results";
        final Request putSystemDataStreamRequest = new Request("PUT", "_data_stream/" + systemDataStreamName);
        putSystemDataStreamRequest.setOptions(
            putSystemDataStreamRequest.getOptions().toBuilder().addHeader("X-elastic-product-origin", "kibana")
        );
        assertOK(client().performRequest(putSystemDataStreamRequest));
        final ObjectPath getDataStream = assertOKAndCreateObjectPath(
            client().performRequest(new Request("GET", "_data_stream/" + systemDataStreamName))
        );
        final String systemDataStreamIndexName = getDataStream.evaluate("data_streams.0.indices.0.index_name");
        assertNumberOfShards(
            systemDataStreamIndexName,
            defaultNumberOfShardsForSystemDataStream,
            RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "kibana")
        );

        // Change cluster wide default has no impact
        randomlyChangeDefaultNumberOfShardsClusterWide();
        for (String systemIndexName : systemIndices) {
            assertNumberOfShards(
                systemIndexName,
                defaultNumberOfShardsForSystemIndices,
                RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE)
            );
        }
        assertNumberOfShards(
            systemDataStreamIndexName,
            defaultNumberOfShardsForSystemDataStream,
            RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "kibana")
        );
    }

    private int randomlyChangeDefaultNumberOfShardsClusterWide() throws IOException {
        final int numberOfShardsNewDefault = randomValueOtherThan(1, () -> randomIntBetween(1, 5));
        updateClusterSettings(
            adminClient(),
            Settings.builder().put("indices.regular_indices.number_of_shards", numberOfShardsNewDefault).build()
        );
        return numberOfShardsNewDefault;
    }

    private void assertNumberOfShards(String indexName, int numberOfShards) throws IOException {
        assertNumberOfShards(indexName, numberOfShards, null);
    }

    private void assertNumberOfShards(String indexName, int numberOfShards, RequestOptions.Builder options) throws IOException {
        final Request getIndexSettingsRequest = new Request("GET", indexName + "/_settings?include_defaults");
        if (options != null) {
            getIndexSettingsRequest.setOptions(options);
        }
        final Response getSettingsResponse = client().performRequest(getIndexSettingsRequest);
        assertOK(getSettingsResponse);
        final Map<String, Object> getSettingsMap = responseAsMap(getSettingsResponse);
        assertThat(getSettingsMap.keySet(), hasSize(1));
        final String actualIndexName = getSettingsMap.keySet().iterator().next();
        assertThat(actualIndexName, startsWith(indexName)); // system indices tend to have a numeric suffix
        assertThat(
            org.elasticsearch.xcontent.ObjectPath.eval("settings.index.number_of_shards", getSettingsMap.get(actualIndexName)),
            equalTo(String.valueOf(numberOfShards))
        );
    }
}
