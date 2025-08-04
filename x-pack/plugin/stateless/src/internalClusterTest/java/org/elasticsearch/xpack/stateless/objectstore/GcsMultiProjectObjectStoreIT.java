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

import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.XContentTestUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class GcsMultiProjectObjectStoreIT extends GoogleObjectStoreTests {

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    protected Settings projectSettings(ProjectId projectId) {
        return Settings.builder()
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.GCS)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "project_" + projectId)
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .build();
    }

    @Override
    protected Settings projectSecrets(ProjectId projectId) {
        return Settings.builder()
            .put(
                "gcs.client.test.credentials_file",
                new String(
                    TestUtils.createServiceAccount(random(), UUID.randomUUID().toString(), "main@project_" + projectId + ".com"),
                    StandardCharsets.UTF_8
                )
            )
            .put(
                "gcs.client.backup.credentials_file",
                new String(
                    TestUtils.createServiceAccount(random(), UUID.randomUUID().toString(), "backup@project_" + projectId + ".com"),
                    StandardCharsets.UTF_8
                )
            )
            .build();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        final var allHandlers = new HashMap<String, GoogleCloudStorageHttpHandler>();
        allHandlers.put("bucket", new GoogleCloudStorageHttpHandler("bucket"));
        allProjects.forEach(projectId -> {
            allHandlers.put("project_" + projectId, new GoogleCloudStorageHttpHandler("project_" + projectId.id()));
            allHandlers.put("backup_" + projectId, new GoogleCloudStorageHttpHandler("backup_" + projectId.id()));
        });
        return Map.of("/", new DelegatingGoogleCloudStorageHttpHandler(Map.copyOf(allHandlers)), "/token", new TestOAuth2HttpHandler());
    }

    @Override
    protected Settings repositorySettings(ProjectId projectId) {
        // TODO: project specific bucket and client settings
        return Settings.builder().put(super.repositorySettings()).put("bucket", "backup_" + projectId.id()).put("client", "backup").build();
    }

    @Override
    protected void assertBackupRepositorySettings(RepositoryMetadata repositoryMetadata, ProjectId projectId) {
        assertThat(repositoryMetadata.settings().get("bucket"), equalTo("backup_" + projectId.id()));
        assertThat(repositoryMetadata.settings().get("client"), equalTo("backup"));
    }

    private static final String CLUSTER_ACCESS_TOKEN = "cluster_access_token";
    private static final String MAIN_ACCESS_TOKEN_PREFIX = "main_access_token_";
    private static final String BACKUP_ACCESS_TOKEN_PREFIX = "backup_access_token_";
    private static final Pattern TOKEN_PATTERN = Pattern.compile("_access_token_([a-zA-Z0-9_-]+)");

    /**
     * This delegating handler allows a single http server to serve multiple Google Cloud Storage buckets. This is needed
     * because, unlike S3, requests to GCS buckets are not prefixed with the bucket name. Therefore, it is not possible
     * to directly configure different routes for different buckets. The delegating handler inspects authentication information
     * from the request header or request body to determine which bucket handler should be used. It also cross-checks
     * between the credentials and the requested path to ensure they are consistent, i.e. clients are using their assigned
     * secrets.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate GCS endpoint")
    private static class DelegatingGoogleCloudStorageHttpHandler implements HttpHandler {

        private final Map<String, GoogleCloudStorageHttpHandler> bucketsHandlers;

        DelegatingGoogleCloudStorageHttpHandler(Map<String, GoogleCloudStorageHttpHandler> bucketsHandlers) {
            this.bucketsHandlers = bucketsHandlers;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String textContainingToken;
            final String contentToMatchRequestPath;

            // Depending on the request, the credentials are in the Authorization header or the request body.
            final String authorization = exchange.getRequestHeaders().getFirst("Authorization");
            if (authorization != null) {
                textContainingToken = authorization;
                contentToMatchRequestPath = exchange.getRequestURI().toString();
            } else {
                final var body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                exchange.setStreams(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)), null);
                textContainingToken = body;
                contentToMatchRequestPath = body;
            }

            final String handlerKey;
            if (textContainingToken.contains("Bearer " + CLUSTER_ACCESS_TOKEN)) {
                handlerKey = "bucket";
                // assert request path and credentials consistency
                assertThat(contentToMatchRequestPath, containsString("bucket"));
            } else {
                final boolean isMainAccessToken = textContainingToken.contains(MAIN_ACCESS_TOKEN_PREFIX);
                assert isMainAccessToken || textContainingToken.contains(BACKUP_ACCESS_TOKEN_PREFIX) : "unexpected: " + textContainingToken;

                final Matcher matcher = TOKEN_PATTERN.matcher(textContainingToken);
                final boolean found = matcher.find();
                assert found : "cannot find project ID in: " + textContainingToken;
                final var projectId = matcher.group(1);
                // assert request path and credentials consistency
                assertThat(contentToMatchRequestPath, containsString((isMainAccessToken ? "project_" : "backup_") + projectId));
                handlerKey = isMainAccessToken ? "project_" + projectId : "backup_" + projectId;
            }

            final var googleCloudStorageHttpHandler = bucketsHandlers.get(handlerKey);
            assert googleCloudStorageHttpHandler != null : "no handler for key: " + handlerKey;
            googleCloudStorageHttpHandler.handle(exchange);
        }
    }

    /**
     * A fake OAuth2 HTTP handler that creates access token based on the JWT issuer. The issuer is an email address
     * that differentiates between cluster clients and project clients (main and backup). The access token is created
     * accordingly to differentiate between them as well. The access tokens are checked in {@link DelegatingGoogleCloudStorageHttpHandler}
     * so that the credentials are verified against the request path and the request is routed to the corresponding
     * bucket handler.
     */
    @SuppressForbidden(reason = "this test uses a HttpServer to emulate GCS token endpoint")
    private static class TestOAuth2HttpHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                final var jwtString = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                final String[] parts = jwtString.split("\\.");
                assert parts.length == 3 : "unexpected JWT: " + jwtString;
                final String payload = parts[1];
                final var jsonMapView = XContentTestUtils.createJsonMapView(
                    Base64.getDecoder().wrap(new ByteArrayInputStream(payload.getBytes(StandardCharsets.UTF_8)))
                );
                final String iss = jsonMapView.get("iss");
                final String accessToken = createAccessToken(iss);

                byte[] response = Strings.format("""
                    {"access_token":"%s","token_type":"Bearer","expires_in":3600}""", accessToken).getBytes(UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.getResponseHeaders().add("Metadata-Flavor", "Google");
                exchange.sendResponseHeaders(RestStatus.OK.getStatus(), response.length);
                exchange.getResponseBody().write(response);
            }
        }

        private static String createAccessToken(String iss) {
            if ("admin@cluster.com".equals(iss)) { // cluster clients
                return CLUSTER_ACCESS_TOKEN;
            } else { // project clients
                final boolean isMainIssuer = iss.startsWith("main@project_");
                assert isMainIssuer || iss.startsWith("backup@project_") : "unexpected issuer: " + iss;

                final var index = iss.indexOf("@project_");
                assert index > 0 : "Unexpected issuer: " + iss;
                final String projectId = iss.substring(index + "@project_".length(), iss.length() - 4);

                return isMainIssuer ? MAIN_ACCESS_TOKEN_PREFIX + projectId : BACKUP_ACCESS_TOKEN_PREFIX + projectId;
            }
        }
    }
}
