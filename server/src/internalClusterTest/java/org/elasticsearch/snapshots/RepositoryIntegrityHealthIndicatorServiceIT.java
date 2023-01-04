/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RepositoryIntegrityHealthIndicatorServiceIT extends AbstractSnapshotIntegTestCase {

    public void testRepositoryIntegrityHealthIndicator() throws IOException, InterruptedException {

        var client = client();

        var repository = "test-repo";
        var location = randomRepoPath();

        createRepository(
            repository,
            "fs",
            Settings.builder()
                .put("location", location)
                .put("compress", false)
                // Don't cache repository data because the test manually modifies the repository data
                .put(BlobStoreRepository.CACHE_REPOSITORY_DATA.getKey(), false)
        );

        assertSnapshotRepositoryHealth("Indicator should be green after empty repository is created", client, GREEN);

        createIndex("test-index-1");
        indexRandomDocs("test-index-1", randomIntBetween(1, 10));
        createFullSnapshot(repository, "snapshot-1");

        assertSnapshotRepositoryHealth("Indicator should be green after successful snapshot is taken", client, GREEN);

        corruptRepository(repository, location);
        // Currently, the health indicator is not proactively checking the repository and
        // instead relies on other operations to detect and flag repository corruption
        assertThat(
            expectThrows(RepositoryException.class, () -> createFullSnapshot(repository, "snapshot-2")).getMessage(),
            containsString("[" + repository + "] The repository has been disabled to prevent data corruption")
        );

        assertSnapshotRepositoryHealth("Indicator should be red after file is deleted from the repository", client, RED);

        deleteRepository(repository);
    }

    private void assertSnapshotRepositoryHealth(String message, Client client, HealthStatus status) {
        var response = client.execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(randomBoolean(), 1000)).actionGet();
        assertThat(message, response.findIndicator(NAME).status(), equalTo(status));
    }

    private void corruptRepository(String name, Path location) throws IOException {
        final RepositoryData repositoryData = getRepositoryData(name);
        Files.delete(location.resolve("index-" + repositoryData.getGenId()));
    }
}
