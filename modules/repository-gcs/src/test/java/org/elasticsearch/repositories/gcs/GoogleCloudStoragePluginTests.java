/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.blobstore.BlobStoreTestUtil;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.junit.Assert;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class GoogleCloudStoragePluginTests extends ESTestCase {

    public void testExposedSettings() {
        List<Setting<?>> settings = new GoogleCloudStoragePlugin(Settings.EMPTY).getSettings();

        Assert.assertEquals(
            List.of(
                "gcs.client.*.credentials_file",
                "gcs.client.*.endpoint",
                "gcs.client.*.project_id",
                "gcs.client.*.connect_timeout",
                "gcs.client.*.read_timeout",
                "gcs.client.*.application_name",
                "gcs.client.*.token_uri",
                "gcs.client.*.proxy.type",
                "gcs.client.*.proxy.host",
                "gcs.client.*.proxy.port"
            ),
            settings.stream().map(Setting::getKey).toList()
        );
    }

    public void testRepositoryProjectId() {
        final var projectId = randomProjectIdOrDefault();
        final var repository = new GoogleCloudStorageRepository(
            projectId,
            new RepositoryMetadata(
                randomIdentifier(),
                GoogleCloudStorageRepository.TYPE,
                Settings.builder()
                    .put(GoogleCloudStorageRepository.BUCKET.getKey(), randomIdentifier())
                    .put(GoogleCloudStorageRepository.BASE_PATH.getKey(), randomIdentifier())
                    .build()
            ),
            NamedXContentRegistry.EMPTY,
            mock(GoogleCloudStorageService.class),
            BlobStoreTestUtil.mockClusterService(),
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            mock(GcsRepositoryStatsCollector.class)
        );
        assertThat(repository.getProjectId(), equalTo(projectId));
    }
}
