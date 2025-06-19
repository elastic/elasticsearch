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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
            .build();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        s3HttpHandler = new ProjectAwareS3HttpHandler(null);
        return Stream.concat(
            Stream.of(Map.entry("/bucket", s3HttpHandler)),
            allProjects.stream().map(projectId -> Map.entry("/project_" + projectId, new ProjectAwareS3HttpHandler(projectId)))
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
    public static class ProjectAwareS3HttpHandler extends InterceptableS3HttpHandler {

        @Nullable
        private final ProjectId projectId;
        private BiPredicate<String, String> authorizationPredicate;

        public ProjectAwareS3HttpHandler(@Nullable ProjectId projectId) {
            super(projectId == null ? "bucket" : "project_" + projectId.id());
            this.projectId = projectId;
            this.authorizationPredicate = AwsCredentialsUtils.fixedAccessKey(
                projectId == null ? "test_access_key" : "test_access_key_" + projectId.id(),
                AwsCredentialsUtils.ANY_REGION,
                "s3"
            );
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (projectId == null) {
                assertThat(exchange.getRequestURI().toString(), startsWith("/bucket"));
            } else {
                assertThat(exchange.getRequestURI().toString(), startsWith("/project_" + projectId.id()));
            }
            AwsCredentialsUtils.checkAuthorization(authorizationPredicate, exchange);
            super.handle(exchange);
        }
    }
}
