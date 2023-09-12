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

import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class S3ObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    private static final Set<String> EXPECTED_REQUEST_NAMES = Set.of(
        "GetObject",
        "ListObjects",
        "PutObject",
        "PutMultipartObject",
        "DeleteObjects",
        "AbortMultipartObject"
    );

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), List.of(S3RepositoryPlugin.class).stream()).toList();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Map.of("/bucket", new S3HttpHandler("bucket"));
    }

    @Override
    protected Settings.Builder nodeSettings() {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("s3.client.test.access_key", "test_access_key");
        mockSecureSettings.setString("s3.client.test.secret_key", "test_secret_key");
        return super.nodeSettings().put("s3.client.test.endpoint", httpServerUrl())
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.S3)
            .put(ObjectStoreService.BUCKET_SETTING.getKey(), "bucket")
            .put(ObjectStoreService.CLIENT_SETTING.getKey(), "test")
            .setSecureSettings(mockSecureSettings);
    }

    @Override
    protected String repositoryType() {
        return "s3";
    }

    @Override
    protected Settings repositorySettings() {
        return Settings.builder()
            .put(super.repositorySettings())
            .put("bucket", "bucket")
            .put("base_path", "backup")
            .put("client", "test")
            .build();
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats repositoryStats) {
        assertEquals(EXPECTED_REQUEST_NAMES, repositoryStats.requestCounts.keySet());
        repositoryStats.requestCounts.forEach((metricName, count) -> {
            if ("AbortMultipartObject".equals(metricName)) {
                assertThat(count, greaterThanOrEqualTo(0L));
            } else {
                assertThat(count, greaterThan(0L));
            }
        });
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats repositoryStats) {
        assertEquals(EXPECTED_REQUEST_NAMES, repositoryStats.requestCounts.keySet());
        repositoryStats.requestCounts.forEach((metricName, count) -> {
            if ("AbortMultipartObject".equals(metricName) || "PutMultipartObject".equals(metricName)) {
                assertThat(count, greaterThanOrEqualTo(0L));
            } else {
                assertThat(count, greaterThan(0L));
            }
        });
    }
}
