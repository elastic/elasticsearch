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

import fixture.gcs.FakeOAuth2HttpHandler;
import fixture.gcs.GoogleCloudStorageHttpHandler;
import fixture.gcs.TestUtils;

import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.gcs.GoogleCloudStoragePlugin;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class GoogleObjectStoreTests extends AbstractMockObjectStoreIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), GoogleCloudStoragePlugin.class);
    }

    @Override
    protected Map<String, HttpHandler> createHttpHandlers() {
        return Map.of("/", new GoogleCloudStorageHttpHandler("bucket"), "/token", new FakeOAuth2HttpHandler());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        final Settings.Builder settings = super.nodeSettings();
        settings.put("gcs.client.test.endpoint", httpServerUrl());
        settings.put("gcs.client.test.token_uri", httpServerUrl() + "/token");
        settings.put("gcs.client.backup.endpoint", httpServerUrl());
        settings.put("gcs.client.backup.token_uri", httpServerUrl() + "/token");
        settings.put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.GCS);
        settings.put(ObjectStoreService.BUCKET_SETTING.getKey(), "bucket");
        settings.put(ObjectStoreService.CLIENT_SETTING.getKey(), "test");

        final byte[] serviceAccount = TestUtils.createServiceAccount(random(), UUID.randomUUID().toString(), "admin@cluster.com");
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile("gcs.client.test.credentials_file", serviceAccount);
        settings.setSecureSettings(mockSecureSettings);

        return settings;
    }

    @Override
    protected String repositoryType() {
        return "gcs";
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

    void assertMetricStats(Set<String> expectedMetrics, Map<String, BlobStoreActionStats> actualMetrics) {
        for (var expectedMetric : expectedMetrics) {
            var actualStats = actualMetrics.get(expectedMetric);
            assertNotNull(expectedMetric + " not found in " + actualMetrics, actualMetrics);
            assertTrue(expectedMetric + " value should be not zero", actualStats.operations() > 0);
        }
    }

    @Override
    protected void assertRepositoryStats(RepositoryStats actualStats, boolean withRandomCrud, OperationPurpose randomPurpose) {
        var expectedMetrics = new HashSet<>(
            List.of(
                "ClusterState_GetObject",
                "ClusterState_InsertObject",
                "ClusterState_ListObjects",
                "Indices_ListObjects",
                "Translog_ListObjects"
            )
        );
        if (withRandomCrud) {
            for (var op : List.of("GetObject", "ListObjects", "InsertObject")) {
                expectedMetrics.add(randomPurpose.getKey() + "_" + op);
            }
        }
        assertMetricStats(expectedMetrics, actualStats.actionStats);
    }

    @Override
    protected void assertObsRepositoryStatsSnapshots(RepositoryStats actualStats) {
        var expectedMetrics = new HashSet<>(
            List.of(
                "SnapshotMetadata_GetObject",
                "SnapshotMetadata_ListObjects",
                "SnapshotMetadata_InsertObject",
                "SnapshotData_ListObjects"
            )
        );
        assertMetricStats(expectedMetrics, actualStats.actionStats);
    }
}
