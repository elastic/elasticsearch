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

import co.elastic.elasticsearch.stateless.ObjectStoreService;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class S3ObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), List.of(S3RepositoryPlugin.class).stream()).toList();
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Collections.singletonMap("/bucket", new S3HttpHandler("bucket"));
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
    protected void assertRepositoryStats(RepositoryStats repositoryStats) {
        final Set<String> expectedRequestNames = Set.of(
            "GetObject",
            "ListObjects",
            "PutObject",
            "PutMultipartObject",
            "DeleteObjects",
            "AbortMultipartObject"
        );
        assertEquals(expectedRequestNames, repositoryStats.requestCounts.keySet());
        repositoryStats.requestCounts.forEach((metricName, count) -> {
            if ("AbortMultipartObject".equals(metricName)) {
                assertThat(metricName + "=" + count, count, equalTo(0L));
            } else {
                assertThat(metricName + "=" + count, count, greaterThan(0L));
            }
        });
    }
}
