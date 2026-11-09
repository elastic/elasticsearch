/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.parquet.docvalues.RepositoryBlobReader;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Spike for the repository-access question of the plan: can a plugin read a byte range of a file that sits at an arbitrary path of a
 * registered repository? The file is written straight to the repository location, so it is not a snapshot blob.
 */
public class RepositoryBlobReaderIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateParquetDocValues.class);
    }

    public void testReadRangeFromArbitraryPathOfFsRepository() throws Exception {
        Path location = randomRepoPath();
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "lake")
                .setType("fs")
                .setSettings(Settings.builder().put("location", location).put("readonly", true))
        );

        byte[] data = randomByteArrayOfLength(randomIntBetween(1024, 1 << 16));
        Path directory = Files.createDirectories(location.resolve("events").resolve("2026"));
        Files.write(directory.resolve("part-0001.parquet"), data);

        RepositoriesService repositories = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class);
        BlobContainer container = RepositoryBlobReader.container(
            repositories,
            ProjectId.DEFAULT,
            "lake",
            BlobPath.EMPTY.add("events").add("2026")
        );

        int position = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - position);
        assertArrayEquals(
            Arrays.copyOfRange(data, position, position + length),
            RepositoryBlobReader.readRange(container, "part-0001.parquet", position, length)
        );
    }
}
