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

import com.azure.storage.common.StorageSharedKeyCredential;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

@LuceneTestCase.SuppressFileSystems("*")
public class AzureMultiProjectObjectStoreIT extends AzureObjectStoreTests {

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    protected Settings projectSettings(ProjectId projectId) {
        return Settings.builder()
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.AZURE)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), testName(projectId))
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .build();
    }

    @Override
    protected Settings projectSecrets(ProjectId projectId) {
        return Settings.builder()
            .put("azure.client.test.account", DEFAULT_ACCOUNT_NAME)
            .put("azure.client.test.key", encodeKey(testName(projectId)))
            .put("azure.client.backup.account", DEFAULT_ACCOUNT_NAME)
            .put("azure.client.backup.key", encodeKey(backupName(projectId)))
            .build();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        final Map<String, AzureHttpHandler> bucketsHandlers = new HashMap<>();
        bucketsHandlers.put(
            "container",
            new AzureHttpHandler(DEFAULT_ACCOUNT_NAME, "container", null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE)
        );
        allProjects.forEach(projectId -> {
            final String testName = testName(projectId);
            bucketsHandlers.put(
                testName,
                new AzureHttpHandler(DEFAULT_ACCOUNT_NAME, testName, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE)
            );
            final String backupName = backupName(projectId);
            bucketsHandlers.put(
                backupName,
                new AzureHttpHandler(DEFAULT_ACCOUNT_NAME, backupName, null, MockAzureBlobStore.LeaseExpiryPredicate.NEVER_EXPIRE)
            );
        });
        return Map.of("/" + DEFAULT_ACCOUNT_NAME, new DelegatingAzureHttpHandler(bucketsHandlers));
    }

    @Override
    protected Settings repositorySettings(ProjectId projectId) {
        return Settings.builder().put(super.repositorySettings()).put("container", backupName(projectId)).put("client", "backup").build();
    }

    @Override
    protected Settings.Builder nodeSettings() {
        final var settingsBuilder = super.nodeSettings();
        final var mockSecureSettings = (MockSecureSettings) settingsBuilder.getSecureSettings();
        mockSecureSettings.setString("azure.client.backup.account", DEFAULT_ACCOUNT_NAME);
        settingsBuilder.put("azure.client.backup.endpoint_suffix", settingsBuilder.get("azure.client.test.endpoint_suffix"));
        return Settings.builder().put(settingsBuilder.build(), false).setSecureSettings(mockSecureSettings);
    }

    @Override
    protected void assertBackupRepositorySettings(RepositoryMetadata repositoryMetadata, ProjectId projectId) {
        assertThat(repositoryMetadata.settings().get("container"), equalTo(backupName(projectId)));
        assertThat(repositoryMetadata.settings().get("client"), equalTo("backup"));
    }

    private static String testName(ProjectId projectId) {
        return "test_" + Objects.requireNonNull(projectId);
    }

    private static String backupName(ProjectId projectId) {
        return "backup_" + Objects.requireNonNull(projectId);
    }

    /**
     * A delegating HTTP handler that routes requests based on the request container to the appropriate {@link AzureHttpHandler}.
     * This is needed because a single AzureHttpHandler can handle for only a single combination of account and container.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an Azure endpoint")
    private static class DelegatingAzureHttpHandler implements HttpHandler {

        private final Map<String, AzureHttpHandler> bucketsHandlers;

        private DelegatingAzureHttpHandler(Map<String, AzureHttpHandler> bucketsHandlers) {
            this.bucketsHandlers = bucketsHandlers;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final var requestPath = exchange.getRequestURI().getPath();

            final String[] parts = requestPath.split("/", 4);
            assert parts.length >= 3 : requestPath;
            assert "".equals(parts[0]) && DEFAULT_ACCOUNT_NAME.equals(parts[1]) : Arrays.toString(parts);
            final var container = parts[2];

            final AzureHttpHandler handler = bucketsHandlers.get(container);
            assert handler != null : bucketsHandlers.keySet() + " does not contain " + container;

            // Validate credentials for requests to project object stores.
            if ("container".equals(container) == false) {
                final var actualAuthHeader = exchange.getRequestHeaders().get("Authorization").getFirst();
                final var storageSharedKeyCredential = new StorageSharedKeyCredential(DEFAULT_ACCOUNT_NAME, encodeKey(container));
                try {
                    final URL requestUrl = new URI(httpServerUrl()).resolve(exchange.getRequestURI()).toURL();
                    final String expectedAuthHeader = storageSharedKeyCredential.generateAuthorizationHeader(
                        requestUrl,
                        exchange.getRequestMethod(),
                        exchange.getRequestHeaders()
                            .entrySet()
                            .stream()
                            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().getFirst()))
                    );
                    assertThat("incorrect auth header for request path " + requestPath, actualAuthHeader, equalTo(expectedAuthHeader));
                } catch (URISyntaxException e) {
                    throw new AssertionError(e);
                }
            }
            handler.handle(exchange);
        }
    }
}
