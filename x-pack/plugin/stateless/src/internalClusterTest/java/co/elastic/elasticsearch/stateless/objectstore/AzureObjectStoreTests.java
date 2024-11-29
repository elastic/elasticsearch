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

package co.elastic.elasticsearch.stateless.objectstore;

import fixture.azure.AzureHttpHandler;
import fixture.azure.MockAzureBlobStore;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.azure.AzureRepositoryPlugin;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.greaterThan;

public class AzureObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    private static final String DEFAULT_ACCOUNT_NAME = "account";
    private static final List<String> EXPECTED_MAIN_STORE_REQUEST_NAMES;
    private static final List<String> EXPECTED_OBS_REQUEST_NAMES;
    static {
        final var mainStorePurposeNames = Set.of("ClusterState");
        final var obsPurposeNames = Set.of("SnapshotMetadata");
        final var operationNames = Set.of("GetBlobProperties", "GetBlob", "ListBlobs", "PutBlob", "BlobBatch");

        EXPECTED_MAIN_STORE_REQUEST_NAMES = Stream.concat(
            Stream.of("Indices_ListBlobs", "Translog_ListBlobs"),
            mainStorePurposeNames.stream().flatMap(p -> operationNames.stream().map(o -> p + "_" + o))
        ).distinct().sorted().toList();

        EXPECTED_OBS_REQUEST_NAMES = Stream.concat(
            Stream.of("SnapshotData_ListBlobs"),
            obsPurposeNames.stream().flatMap(p -> operationNames.stream().map(o -> p + "_" + o))
        ).distinct().sorted().toList();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), List.of(AzureRepositoryPlugin.class).stream()).toList();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Map.of(
            "/" + DEFAULT_ACCOUNT_NAME,
            new AzureHttpHandler(DEFAULT_ACCOUNT_NAME, "container", null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE)
        );
    }

    @Override
    protected Settings.Builder nodeSettings() {
        final String key = Base64.getEncoder().encodeToString(randomAlphaOfLength(14).getBytes(StandardCharsets.UTF_8));
        String accountName = DEFAULT_ACCOUNT_NAME;

        final String endpoint = "ignored;DefaultEndpointsProtocol=http;BlobEndpoint=" + httpServerUrl() + "/" + accountName;
        Settings.Builder settings = super.nodeSettings().put("azure.client.test.endpoint_suffix", endpoint)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.AZURE)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "container")
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test");

        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("azure.client.test.account", accountName);
        if (randomBoolean()) {
            mockSecureSettings.setString("azure.client.test.key", key);
        } else {
            // The SDK expects a valid SAS TOKEN
            mockSecureSettings.setString("azure.client.test.sas_token", "se=2021-07-20T13%3A21Z&sp=rwdl&sv=2018-11-09&sr=c&sig=random");
        }
        settings.setSecureSettings(mockSecureSettings);

        return settings;
    }

    @Override
    protected String repositoryType() {
        return "azure";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put("container", "container")
            .put("base_path", "backup")
            .put("client", "test")
            .build();
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats repositoryStats) {
        assertThat(
            repositoryStats.requestCounts.keySet().stream().sorted().toList(),
            contains(EXPECTED_MAIN_STORE_REQUEST_NAMES.toArray(new String[] {}))
        );
        repositoryStats.requestCounts.values().forEach(count -> assertThat(count, greaterThan(0L)));
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats) {
        assertThat(
            repositoryStats.requestCounts.keySet().stream().sorted().toList(),
            contains(EXPECTED_OBS_REQUEST_NAMES.toArray(new String[] {}))
        );
        repositoryStats.requestCounts.values().forEach(count -> assertThat(count, greaterThan(0L)));
    }
}
