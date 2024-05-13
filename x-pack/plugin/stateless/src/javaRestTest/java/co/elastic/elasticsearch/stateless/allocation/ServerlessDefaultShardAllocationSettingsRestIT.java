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

package co.elastic.elasticsearch.stateless.allocation;

import co.elastic.elasticsearch.serverless.constants.ProjectType;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.serverless.ServerlessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.allocation.StatelessIndexSettingProvider.DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING;
import static java.lang.Integer.parseInt;
import static org.elasticsearch.cluster.metadata.IndexMetadata.PER_INDEX_MAX_NUMBER_OF_SHARDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests the default number of shards for different types of indexes in serverless when the
 * DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING setting is engaged.
 *
 * Only regular indexes (not data stream or system indexes) should be affected by the setting.
 * The setting is only active when set to a value > 0, and should only be used if a create
 * index request does not specify the number of shards.
 */
public class ServerlessDefaultShardAllocationSettingsRestIT extends ESRestTestCase {
    private static final String TEST_OPERATOR_USER = "elastic-operator-user";
    private static final String TEST_USER = "elastic-regular-user";
    private static final String TEST_PASSWORD = "elastic-password";
    private static final int TEST_MAX_SHARDS = 10;

    @ClassRule
    public static ElasticsearchCluster cluster = ServerlessElasticsearchCluster.local()
        .name("javaRestTest")
        .user(TEST_OPERATOR_USER, TEST_PASSWORD)
        .user(TEST_USER, TEST_PASSWORD, User.ROOT_USER_ROLE, false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(TEST_OPERATOR_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(TEST_USER, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private int randomlySetADefaultNumberOfShardsForNewRegularIndicesOverride() throws IOException {
        // Activate the override by setting a value > 0. The max allowable value is PER_INDEX_MAX_NUMBER_OF_SHARDS, but, for CI stability,
        // we shall cap the number of shards at TEST_MAX_SHARDS.
        final int numberOfShardsNewDefault = randomIntBetween(1, TEST_MAX_SHARDS);
        updateClusterSettings(
            adminClient(),
            Settings.builder().put(DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING.getKey(), numberOfShardsNewDefault).build()
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
        final Response getSettingsResponse = adminClient().performRequest(getIndexSettingsRequest);
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

    public void testZeroDefaultNumberOfShardsDoesNothing() throws IOException {
        switch (between(0, 2)) {
            case 0: // Deactivate the override by setting it to 0.
                updateClusterSettings(
                    adminClient(),
                    Settings.builder().put(DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING.getKey(), 0).build()
                );
                break;
            case 1: // Activate the override by setting to a number, then set to null to deactivate it.
                randomlySetADefaultNumberOfShardsForNewRegularIndicesOverride();
                updateClusterSettings(
                    adminClient(),
                    Settings.builder().putNull(DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING.getKey()).build()
                );
                break;
            default: // Do nothing to test the inactive setting default of 0.
        }

        // The default number of shards for a new index (if shard number is unspecified in the create index request) in serverless uses the
        // PROJECT_TYPE setting, which defaults to ProjectType.ELASTICSEARCH_SEARCH.
        final int defaultNumberOfShardsForRegularIndices = ProjectType.ELASTICSEARCH_SEARCH.getNumberOfShards();
        final String indexName = randomIdentifier();
        assertOK(client().performRequest(new Request("PUT", indexName)));
        assertNumberOfShards(indexName, defaultNumberOfShardsForRegularIndices);
    }

    public void testDefaultNumberOfShardsForRegularIndicesOverrides() throws Exception {
        // The default number of shards for a new index (if shard number is unspecified in the create index request) in serverless uses the
        // PROJECT_TYPE setting, which defaults to ProjectType.ELASTICSEARCH_SEARCH.
        final int defaultNumberOfShardsForRegularIndices = ProjectType.ELASTICSEARCH_SEARCH.getNumberOfShards();
        final String projectDefaultIndexName = randomIdentifier();
        assertOK(client().performRequest(new Request("PUT", projectDefaultIndexName)));
        assertNumberOfShards(projectDefaultIndexName, defaultNumberOfShardsForRegularIndices);

        // A user (with operator level access) can make a create index request specifying the number of shards, overriding the ProjectType
        // default.
        final String userIndexName = randomIdentifier();
        final int userSpecifiedNumberOfShardsForCreateIndex = randomValueOtherThan(
            defaultNumberOfShardsForRegularIndices,
            () -> randomIntBetween(1, TEST_MAX_SHARDS)
        );
        {
            final Request putIndexRequest = new Request("PUT", userIndexName);
            putIndexRequest.setJsonEntity(Strings.format("""
                {
                  "settings": {
                    "number_of_shards": %s
                  }
                }""", userSpecifiedNumberOfShardsForCreateIndex));
            assertOK(adminClient().performRequest(putIndexRequest));
            assertNumberOfShards(userIndexName, userSpecifiedNumberOfShardsForCreateIndex);
        }

        // The ProjectType default number of shards for new indexes can be overridden (with operator level access) by changing
        // DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING.
        final int createIndexDefaultNumberOfShards = randomlySetADefaultNumberOfShardsForNewRegularIndicesOverride();
        final String anotherIndexName = randomIdentifier();
        assertOK(client().performRequest(new Request("PUT", anotherIndexName)));
        assertNumberOfShards(anotherIndexName, createIndexDefaultNumberOfShards);

        // All previously created indices are *not* affected by the new cluster wide default for new indexes.
        assertNumberOfShards(projectDefaultIndexName, defaultNumberOfShardsForRegularIndices);
        assertNumberOfShards(userIndexName, userSpecifiedNumberOfShardsForCreateIndex);

        // Ensure that a user create index request 'number_of_shards' still takes precedence over the 'default_number_of_shards' server
        // setting.
        final String secondUserIndexName = randomIdentifier();
        {
            final Request putIndexRequest = new Request("PUT", secondUserIndexName);
            putIndexRequest.setJsonEntity(Strings.format("""
                {
                  "settings": {
                    "number_of_shards": %s
                  }
                }""", userSpecifiedNumberOfShardsForCreateIndex));
            assertOK(adminClient().performRequest(putIndexRequest));
            assertNumberOfShards(secondUserIndexName, userSpecifiedNumberOfShardsForCreateIndex);
        }
    }

    public void testDefaultNumberOfShardsForRegularIndicesDoesNotChangeDataStreamSingleShardDefault() throws IOException {
        // Engaging the default override for new regular indices has no effect on new data streams.
        randomlySetADefaultNumberOfShardsForNewRegularIndicesOverride();

        // Data streams default to a single shard. The serverless ProjectType doesn't matter.
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
    }

    public void testDefaultNumberOfShardsForRegularIndicesDoesNotChangeSystemIndicesSingleShardDefault() throws IOException {
        // Engaging the default override for new regular indices has no effect on a new system data stream.
        randomlySetADefaultNumberOfShardsForNewRegularIndicesOverride();

        // System indices default to a single primary shard if not explicitly specified otherwise. The serverless ProjectType default is not
        // applied.
        final int defaultNumberOfShardsForSystemIndices = 1;
        // Pick some system indices to create. Not all of them have an explicit number_of_shards setting. Some are aliases while others are
        // concrete index names.
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

        // Similarly, a system data stream will default to a single shard.
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
    }

    public void testDefaultNumberOfShardOverrideIsOperatorOnly() {
        // Cannot specify the number of shards in a create index request as a non-operator user.
        final int defaultNumberOfShardsForRegularIndices = ProjectType.ELASTICSEARCH_SEARCH.getNumberOfShards();
        final String indexNameWithPerIndexOverride = randomIdentifier();
        final Request putIndexRequest = new Request("PUT", indexNameWithPerIndexOverride);
        final int createIndexNumberOfShardsOverride = randomValueOtherThan(
            defaultNumberOfShardsForRegularIndices,
            () -> randomIntBetween(1, parseInt(PER_INDEX_MAX_NUMBER_OF_SHARDS))
        );
        putIndexRequest.setJsonEntity(Strings.format("""
            {
              "settings": {
                "number_of_shards": %s
              }
            }""", createIndexNumberOfShardsOverride));
        assertThrows(ResponseException.class, () -> client().performRequest(putIndexRequest));

        // Cannot override the server setting as a non-operator user.
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> updateClusterSettings(
                client(),
                Settings.builder().put(DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING.getKey(), 1).build()
            )
        );
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(RestStatus.GONE.getStatus()));
    }
}
