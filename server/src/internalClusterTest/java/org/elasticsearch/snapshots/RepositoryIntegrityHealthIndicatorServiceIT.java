/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.Diagnosis.Resource.Type;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.getRepositoryDataBlobName;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.ALL_REPOS_HEALTHY;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.CORRUPTED_DEFINITION;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.IMPACTS;
import static org.elasticsearch.snapshots.RepositoryIntegrityHealthIndicatorService.NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RepositoryIntegrityHealthIndicatorServiceIT extends AbstractSnapshotIntegTestCase {

    public void testRepositoryIntegrityHealthIndicator() throws IOException {
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

        assertSnapshotRepositoryHealth("Indicator should be green after empty repository is created", greenResult(1));

        createIndex("test-index-1");
        indexRandomDocs("test-index-1", randomIntBetween(1, 10));
        createFullSnapshot(repository, "snapshot-1");

        assertSnapshotRepositoryHealth("Indicator should be green after successful snapshot is taken", greenResult(1));

        corruptRepository(repository, location);
        // Currently, the health indicator is not proactively checking the repository and
        // instead relies on other operations to detect and flag repository corruption
        assertThat(
            expectThrows(RepositoryException.class, () -> createFullSnapshot(repository, "snapshot-2")).getMessage(),
            containsString("[" + repository + "] The repository has been disabled to prevent data corruption")
        );

        assertSnapshotRepositoryHealth(
            "Indicator should be yellow after file is deleted from the repository",
            new HealthIndicatorResult(
                NAME,
                YELLOW,
                "Detected [1] corrupted snapshot repository.",
                new SimpleHealthIndicatorDetails(
                    Map.of(
                        "total_repositories",
                        1,
                        "corrupted_repositories",
                        1,
                        "corrupted",
                        List.of(repository),
                        "unknown_repositories",
                        0,
                        "invalid_repositories",
                        0
                    )
                ),
                IMPACTS,
                List.of(new Diagnosis(CORRUPTED_DEFINITION, List.of(new Diagnosis.Resource(Type.SNAPSHOT_REPOSITORY, List.of(repository)))))
            )
        );

        deleteRepository(repository);
    }

    private void assertSnapshotRepositoryHealth(String message, HealthIndicatorResult expected) {
        var response = client().execute(GetHealthAction.INSTANCE, new GetHealthAction.Request(NAME, true, 1000)).actionGet();
        assertThat(message, response.findIndicator(NAME), equalTo(expected));
    }

    private static HealthIndicatorResult greenResult(int totalRepositories) {
        return new HealthIndicatorResult(
            NAME,
            GREEN,
            ALL_REPOS_HEALTHY,
            new SimpleHealthIndicatorDetails(Map.of("total_repositories", totalRepositories)),
            List.of(),
            List.of()
        );
    }

    private void corruptRepository(String name, Path location) throws IOException {
        final RepositoryData repositoryData = getRepositoryData(name);
        Files.delete(location.resolve(getRepositoryDataBlobName(repositoryData.getGenId())));
    }
}
