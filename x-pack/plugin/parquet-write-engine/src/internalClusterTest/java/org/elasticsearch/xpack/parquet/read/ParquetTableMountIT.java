/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.read;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.fs.FsBlobStore;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.test.JnaCleanerThreadsFilter;
import org.elasticsearch.test.NettyGlobalThreadsFilter;
import org.elasticsearch.xpack.parquet.read.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.read.mount.ParquetMountRequest;
import org.elasticsearch.xpack.parquet.read.mount.PreviewParquetAction;
import org.elasticsearch.xpack.parquet.write.ParquetFlushIT;
import org.elasticsearch.xpack.parquet.write.ParquetWriteEnginePlugin;
import org.elasticsearch.xpack.parquet.write.table.RepositoryFileIO;
import org.elasticsearch.xpack.parquet.write.table.RepositoryTableOperations;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

/**
 * An index can mount an Iceberg table that the write engine did not write, and follows it: a file that another writer appends becomes
 * searchable after a refresh. The table here is written through Iceberg's API with file names of its own and no owner, as another tool
 * would write it.
 */
@ThreadLeakFilters(
    filters = {
        GraalVMThreadsFilter.class,
        NettyGlobalThreadsFilter.class,
        JnaCleanerThreadsFilter.class,
        ParquetFlushIT.IcebergWorkerPoolFilter.class }
)
// one data node, as the other mount tests: a preview right after the repository is registered did not find it on a second node
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class ParquetTableMountIT extends ESIntegTestCase {

    private static final String REPOSITORY = "lake";
    private static final String TABLE = "warehouse/events";
    private static final String INDEX = "events";
    private static final int ROWS = 2_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ParquetWriteEnginePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        // only one engine factory may apply to an index, and ours must be the one
        return false;
    }

    /** A table in the directory of an {@code fs} repository, which the test writes through Iceberg's API. */
    private record TestTable(FsBlobStore store, BlobPath blobPath, String rootUri, BaseTable table) {

        static TestTable create(Path repositoryLocation) throws IOException {
            final FsBlobStore store = new FsBlobStore(4096, repositoryLocation, false);
            final BlobPath blobPath = BlobPath.EMPTY.add("warehouse").add("events");
            final String rootUri = repositoryLocation.resolve("warehouse").resolve("events").toUri().toString().replaceAll("/+$", "");
            final RepositoryTableOperations operations = new RepositoryTableOperations(new RepositoryFileIO(store, blobPath, rootUri));
            operations.commit(
                null,
                TableMetadata.newTableMetadata(new Schema(), PartitionSpec.unpartitioned(), SortOrder.unsorted(), rootUri, Map.of())
            );
            return new TestTable(store, blobPath, rootUri, new BaseTable(operations, rootUri));
        }

        /** Writes a data file with the name that another tool would give it, and appends it to the table. */
        void append(String name, byte[] parquet, long rows) throws IOException {
            store.blobContainer(blobPath.add("data")).writeBlob(RepositoryFileIO.PURPOSE, name, new BytesArray(parquet), true);
            table.newAppend()
                .appendFile(
                    DataFiles.builder(PartitionSpec.unpartitioned())
                        .withPath(rootUri + "/data/" + name)
                        .withFormat(FileFormat.PARQUET)
                        .withFileSizeInBytes(parquet.length)
                        .withRecordCount(rows)
                        .build()
                )
                .commit();
        }
    }

    private long count() {
        final SearchResponse response = prepareSearch(INDEX).setSize(0).setTrackTotalHits(true).get();
        try {
            assertThat(response.getFailedShards(), equalTo(0));
            return response.getHits().getTotalHits().value();
        } finally {
            response.decRef();
        }
    }

    public void testAMountFollowsTheTable() throws Exception {
        final Path location = randomRepoPath();
        final TestTable table = TestTable.create(location);
        table.append("part-00000-a.parquet", ParquetTestFiles.threeColumnFile(ROWS), ROWS);
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", location).put("readonly", true))
        );

        final Map<String, Object> preview = client().execute(
            PreviewParquetAction.INSTANCE,
            new ParquetMountRequest(Map.of("repository", REPOSITORY, "table", TABLE))
        ).actionGet().body();
        assertThat(preview.get("row_count"), equalTo((long) ROWS));
        assertThat(preview.get("data_files"), equalTo(1));

        client().execute(
            MountParquetAction.INSTANCE,
            new ParquetMountRequest(
                Map.of(
                    "repository",
                    REPOSITORY,
                    "table",
                    TABLE,
                    "index",
                    INDEX,
                    "fields",
                    Map.of("id", Map.of("type", "long"), "n", Map.of("name", "number")),
                    "index_settings",
                    // no scheduled refresh: the test refreshes when it wants the index to read the table
                    Map.of("index.number_of_replicas", 0, "index.refresh_interval", "-1")
                )
            )
        ).actionGet();
        ensureGreen(INDEX);
        // the mount reads the table when the shard starts
        indicesAdmin().prepareRefresh(INDEX).get();
        assertThat(count(), equalTo((long) ROWS));
        assertResponse(
            prepareSearch(INDEX).setSize(0)
                .setTrackTotalHits(true)
                .setQuery(QueryBuilders.termQuery("name", "name-7"))
                .addAggregation(AggregationBuilders.max("max_id").field("id"))
                .addAggregation(AggregationBuilders.max("max_number").field("number")),
            response -> assertThat(response.getHits().getTotalHits().value(), equalTo(1L))
        );

        // another writer appends a file: a refresh of the index makes its rows searchable
        table.append("part-00001-b.parquet", ParquetTestFiles.threeColumnFile(ROWS / 2), ROWS / 2);
        assertThat("not before a refresh", count(), equalTo((long) ROWS));
        indicesAdmin().prepareRefresh(INDEX).get();
        assertThat(count(), equalTo((long) ROWS + ROWS / 2));
        assertResponse(
            prepareSearch(INDEX).setSize(0).addAggregation(AggregationBuilders.max("max_id").field("id")),
            response -> assertThat(((Max) response.getAggregations().get("max_id")).value(), equalTo((ROWS - 1) * 7.0))
        );

        // nothing tells a mount of the commits of another writer: its scheduled refresh reads the table, every refresh interval
        updateIndexSettings(Settings.builder().put("index.refresh_interval", "100ms"), INDEX);
        table.append("part-00002-c.parquet", ParquetTestFiles.threeColumnFile(ROWS / 4), ROWS / 4);
        assertBusy(() -> assertThat(count(), equalTo((long) ROWS + ROWS / 2 + ROWS / 4)));
    }
}
