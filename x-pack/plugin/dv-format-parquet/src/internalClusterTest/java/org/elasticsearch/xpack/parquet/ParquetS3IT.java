/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import fixture.s3.S3ConsistencyModel;
import fixture.s3.S3HttpFixture;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.s3.S3RepositoryPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.parquet.docvalues.ParquetTestFiles;
import org.elasticsearch.xpack.parquet.docvalues.RepositoryBlobReader;
import org.junit.ClassRule;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * S3 spike: does a mount read its Parquet file through a {@code repository-s3} repository? The repository talks to the S3 fixture, which
 * stands in for a bucket. The repository that serves the mount is read-only, the same as for the public ClickBench bucket, and it is
 * a second registration of the base path that a writable repository has filled, so the test also covers a repository name that differs
 * from the one that wrote the file.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetS3IT extends ESIntegTestCase {

    private static final String BUCKET = "bucket";
    private static final String BASE_PATH = "base_path_integration_tests";

    @ClassRule
    public static final S3HttpFixture s3Fixture = new S3HttpFixture(true, S3ConsistencyModel::randomConsistencyModel);

    @Override
    protected boolean addMockInternalEngine() {
        // only one engine factory may apply to an index, and ours must be the one
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateParquetDocValues.class, S3RepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("s3.client.default.access_key", "s3_test_access_key");
        secureSettings.setString("s3.client.default.secret_key", "s3_test_secret_key");
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("s3.client.default.endpoint", s3Fixture.getAddress())
            .put("s3.client.default.region", "us-east-1")
            .setSecureSettings(secureSettings)
            .build();
    }

    /** Fills the bucket through a writable repository, and registers the read-only repository that the mount uses. */
    private void writeFileAndRegisterRepository() throws Exception {
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "writer")
                .setType("s3")
                .setSettings(Settings.builder().put("bucket", BUCKET).put("base_path", BASE_PATH))
        );
        RepositoriesService repositories = internalCluster().getCurrentMasterNodeInstance(RepositoriesService.class);
        // the container of a blob store starts at the root of the bucket, and the base path of the repository is added by the caller
        BlobPath events = ((BlobStoreRepository) repositories.repository(ProjectId.DEFAULT, "writer")).basePath().add("events");
        BlobContainer container = RepositoryBlobReader.container(repositories, ProjectId.DEFAULT, "writer", events);
        byte[] file = ParquetTestFiles.writeFlat(ParquetMounts.ROWS, ParquetMounts.ROWS_PER_ROW_GROUP);
        container.writeBlob(OperationPurpose.SNAPSHOT_DATA, "part-0001.parquet", new BytesArray(file), true);

        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, ParquetMounts.REPOSITORY)
                .setType("s3")
                .setSettings(Settings.builder().put("bucket", BUCKET).put("base_path", BASE_PATH).put("readonly", true))
        );
    }

    public void testAMountReadsItsFileFromS3() throws Exception {
        internalCluster().startNode();
        writeFileAndRegisterRepository();
        assertAcked(
            prepareCreate("events").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(ParquetMounts.mountSettings().build())
            ).setMapping(ParquetMounts.MAPPING)
        );
        ensureGreen("events");
        assertServesTheFile();

        // the stub must also open again without a new attach, and read the file through the repository once more
        internalCluster().fullRestart();
        ensureGreen("events");
        assertServesTheFile();
    }

    private void assertServesTheFile() {
        int rows = ParquetMounts.ROWS;
        assertResponse(
            prepareSearch("events").setSize(0)
                .setTrackTotalHits(true)
                .addAggregation(AggregationBuilders.max("max_status").field("status"))
                .addAggregation(AggregationBuilders.sum("sum_ts").field("ts")),
            response -> {
                assertEquals(rows, response.getHits().getTotalHits().value());
                assertEquals(rows - 1, ((Max) response.getAggregations().get("max_status")).value(), 0.0);
                assertEquals(1000.0 * rows * (rows - 1) / 2, ((Sum) response.getAggregations().get("sum_ts")).value(), 0.0);
            }
        );
    }
}
