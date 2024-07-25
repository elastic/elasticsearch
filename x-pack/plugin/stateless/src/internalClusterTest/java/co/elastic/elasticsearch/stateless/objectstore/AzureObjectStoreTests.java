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

import com.sun.net.httpserver.HttpHandler;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.azure.AzureRepositoryPlugin;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThan;

@LuceneTestCase.AwaitsFix(bugUrl = "https://elasticco.atlassian.net/browse/ES-5680")
// TODO: When the above AwaitsFix is removed, we will want to ensure the assertRepositoryStats method actually asserts correctly.
// It was added while this test class is muted so that the assertions are never exercised. Specifically, the assertions state
// that every type of object store requests is performed at least once. This may or may not be true for request types like
// PutBlock and PutBlockList.
public class AzureObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    private static final String DEFAULT_ACCOUNT_NAME = "account";
    private static final Set<String> EXPECTED_REQUEST_NAMES = Set.of(
        "GetBlob",
        "ListBlobs",
        "GetBlobProperties",
        "PutBlob",
        "PutBlock",
        "PutBlockList"
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), List.of(AzureRepositoryPlugin.class).stream()).toList();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/" + DEFAULT_ACCOUNT_NAME, new AzureHttpHandler(DEFAULT_ACCOUNT_NAME, "container", null));
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
            .put("bucket", "container")
            .put("base_path", "backup")
            .put("client", "test")
            .build();
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats repositoryStats) {
        assertEquals(EXPECTED_REQUEST_NAMES, repositoryStats.requestCounts.keySet());
        repositoryStats.requestCounts.values().forEach(count -> assertThat(count, greaterThan(0L)));
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats) {
        assertEquals(EXPECTED_REQUEST_NAMES, repositoryStats.requestCounts.keySet());
        repositoryStats.requestCounts.values().forEach(count -> assertThat(count, greaterThan(0L)));
    }
}
