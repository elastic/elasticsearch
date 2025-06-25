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

import fixture.aws.AwsCredentialsUtils;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

@SuppressForbidden(reason = "this test uses a HttpServer to emulate a cloud-based storage service")
public class S3MultiProjectObjectStoreIT extends S3ObjectStoreTests {

    @Override
    protected boolean multiProjectIntegrationTest() {
        return true;
    }

    @Override
    protected Settings projectSettings(ProjectId projectId) {
        return Settings.builder()
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.S3)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "project_" + projectId)
            .put(ObjectStoreService.BASE_PATH_SETTING.getKey(), "base_path")
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .build();
    }

    @Override
    protected Settings projectSecrets(ProjectId projectId) {
        return Settings.builder()
            .put("s3.client.test.access_key", "test_access_key_" + projectId.id())
            .put("s3.client.test.secret_key", "test_secret_key_" + projectId.id())
            .put("s3.client.backup.access_key", "backup_access_key_" + projectId.id())
            .put("s3.client.backup.secret_key", "backup_secret_key_" + projectId.id())
            .build();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        s3HttpHandler = new ProjectAwareS3HttpHandler(null, "bucket", "test_access_key");
        final Map<String, HttpHandler> allHandlers = new HashMap<>();
        allHandlers.put("/bucket", s3HttpHandler);
        allProjects.forEach(projectId -> {
            allHandlers.put(
                "/project_" + projectId,
                new ProjectAwareS3HttpHandler(projectId, "project_" + projectId.id(), "test_access_key_" + projectId.id())
            );
            allHandlers.put(
                "/backup_" + projectId,
                new ProjectAwareS3HttpHandler(projectId, "backup_" + projectId.id(), "backup_access_key_" + projectId.id())
            );
        });
        return Collections.unmodifiableMap(allHandlers);
    }

    @Override
    protected Settings repositorySettings(ProjectId projectId) {
        return Settings.builder().put(super.repositorySettings()).put("bucket", "backup_" + projectId.id()).put("client", "backup").build();
    }

    @Override
    protected void assertBackupRepositorySettings(RepositoryMetadata repositoryMetadata, ProjectId projectId) {
        assertThat(repositoryMetadata.settings().get("bucket"), equalTo("backup_" + projectId.id()));
        assertThat(repositoryMetadata.settings().get("base_path"), equalTo("backup"));
        assertThat(repositoryMetadata.settings().get("client"), equalTo("backup"));
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    public static class ProjectAwareS3HttpHandler extends InterceptableS3HttpHandler {

        @Nullable
        private final ProjectId projectId;
        private final BiPredicate<String, String> authorizationPredicate;
        private final String accessKey;

        public ProjectAwareS3HttpHandler(@Nullable ProjectId projectId, String bucket, String accessKey) {
            super(bucket);
            this.projectId = projectId;
            this.accessKey = accessKey;
            this.authorizationPredicate = AwsCredentialsUtils.fixedAccessKey(accessKey, AwsCredentialsUtils.ANY_REGION, "s3");
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            final String requestUri = exchange.getRequestURI().toString();
            if (projectId == null) {
                assertThat(requestUri, startsWith("/bucket"));
            } else {
                if (accessKey.startsWith("backup_")) {
                    assertThat(requestUri, startsWith("/backup_" + projectId.id()));
                } else {
                    assertThat(requestUri, startsWith("/project_" + projectId.id()));
                }
            }
            AwsCredentialsUtils.checkAuthorization(authorizationPredicate, exchange);
            super.handle(exchange);
        }
    }
}
