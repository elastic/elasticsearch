/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractExternalDataSourceIT;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.parquet.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.mount.ParquetMountRequest;
import org.elasticsearch.xpack.parquet.mount.PreviewParquetAction;
import org.elasticsearch.xpack.parquet.repository.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.repository.ParquetBlobCache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Mounts a Parquet file through an ES|QL data source, with the ES|QL plugin loaded next to this one: the data source of type
 * {@code local} reads the file from the disk of the node, so no store has to be faked. The same file is also a dataset of the same
 * data source, which ES|QL reads with its own Parquet reader and no doc values format, so the mount can be checked against it.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ParquetDataSourceMountIT extends AbstractExternalDataSourceIT {

    /** The name of the data source that {@code registerLocalFileDataset} creates, with the type {@code local}. */
    private static final String DATA_SOURCE = "file_ds";

    @Override
    protected boolean addMockInternalEngine() {
        // only one engine factory may apply to an index, and ours must be the one
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        // the disk cache is not used on a node with several data paths
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        // our plugin, and the ES|QL Parquet reader that serves the dataset that the mount is compared with
        return List.of(ParquetDocValuesPlugin.class, ParquetDataSourcePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(ParquetBlobCache.CACHE_SIZE.getKey(), "32mb")
            .put(ParquetBlobCache.CACHE_REGION_SIZE.getKey(), "1mb")
            .build();
    }

    /** Writes the file under the temp root that the ES|QL local access allow-list covers, and makes a dataset of it. */
    private String writeFileAndRegisterDataSource() throws IOException {
        Path file = createTempDir().resolve("events.parquet");
        Files.write(file, ParquetSampleFiles.writeFlat(ParquetMounts.ROWS, ParquetMounts.ROWS_PER_ROW_GROUP));
        String uri = file.toUri().toString();
        registerLocalFileDataset("events_ds", uri, Map.of());
        return uri;
    }

    private static ParquetMountRequest mountOf(Object dataSource, Object path) {
        return new ParquetMountRequest(
            Map.of(
                "data_source",
                dataSource,
                "paths",
                List.of(path),
                "index",
                ParquetMounts.INDEX,
                "index_settings",
                Map.of("index.number_of_replicas", 0)
            )
        );
    }

    public void testAMountThroughADataSourceServesTheFileAndEqualsTheDatasetOfTheSameFile() throws Exception {
        String uri = writeFileAndRegisterDataSource();
        client().execute(MountParquetAction.INSTANCE, mountOf(DATA_SOURCE, uri)).actionGet();
        ensureGreen(ParquetMounts.INDEX);

        // the index names the data source and no repository, and its pieces name the complete URI
        Settings index = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, ParquetMounts.INDEX)
            .get()
            .getIndexToSettings()
            .get(ParquetMounts.INDEX);
        assertThat(ParquetBackedSettings.DATA_SOURCE.get(index), equalTo(DATA_SOURCE));
        assertFalse(ParquetBackedSettings.REPOSITORY.exists(index));
        assertThat(ParquetBackedSettings.pieceSpecs(index).get(0).path(), equalTo(uri));

        // Query DSL aggregations read the doc values through the data source
        ParquetMounts.assertServesTheFile();

        // ES|QL reads the mount through the doc values and the dataset through its own reader, and the two agree
        String stats = " | STATS rows = COUNT(*), max_id = MAX(id), sum_ts = SUM(ts), sum_low = SUM(low), names = COUNT_DISTINCT(name)";
        List<List<Object>> mounted;
        try (var response = run("FROM " + ParquetMounts.INDEX + stats)) {
            mounted = getValuesList(response);
        }
        List<List<Object>> dataset;
        try (var response = run("FROM events_ds" + stats)) {
            dataset = getValuesList(response);
        }
        assertThat(mounted, equalTo(dataset));
        assertThat(((Number) mounted.get(0).get(0)).longValue(), equalTo((long) ParquetMounts.ROWS));

        // a string filter and a grouping, which read the binary doc values
        String grouped = " | WHERE kind == \"k3\" | STATS rows = COUNT(*), max_id = MAX(id) BY kind";
        List<List<Object>> mountedGroup;
        try (var response = run("FROM " + ParquetMounts.INDEX + grouped)) {
            mountedGroup = getValuesList(response);
        }
        List<List<Object>> datasetGroup;
        try (var response = run("FROM events_ds" + grouped)) {
            datasetGroup = getValuesList(response);
        }
        assertThat(mountedGroup, equalTo(datasetGroup));
        assertThat(mountedGroup.size(), equalTo(1));

        // the disk cache keyed the file by the stored location, which names the data source
        // the node that holds the shard, as the cluster may have a client node too
        SharedBlobCacheService.Stats cacheStats = internalCluster().getDataNodeInstance(ParquetBlobCache.class).stats();
        assertThat("the reads went through the cache", cacheStats.writeBytes(), greaterThan(0L));
    }

    public void testAPreviewThroughADataSourceNeedsNoIndex() throws Exception {
        String uri = writeFileAndRegisterDataSource();
        var response = client().execute(
            PreviewParquetAction.INSTANCE,
            new ParquetMountRequest(Map.of("data_source", DATA_SOURCE, "paths", List.of(uri)))
        ).actionGet();
        assertThat(((Number) response.body().get("row_count")).longValue(), equalTo((long) ParquetMounts.ROWS));
    }

    public void testAMountRejectsWhatTheDataSourceCannotReadWithAReason() throws Exception {
        String uri = writeFileAndRegisterDataSource();

        Exception unknown = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(MountParquetAction.INSTANCE, mountOf("no_such_source", uri)).actionGet()
        );
        assertThat(unknown.getMessage(), containsString("data source [no_such_source] does not exist"));

        // the type local reads the scheme file and no other
        Exception scheme = expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(MountParquetAction.INSTANCE, mountOf(DATA_SOURCE, "s3://bucket/events.parquet")).actionGet()
        );
        assertThat(scheme.getMessage(), containsString("is not a file that the data source [" + DATA_SOURCE + "] reads"));

        Exception missing = expectThrows(
            ResourceNotFoundException.class,
            () -> client().execute(MountParquetAction.INSTANCE, mountOf(DATA_SOURCE, uri.replace("events.parquet", "missing.parquet")))
                .actionGet()
        );
        assertThat(missing.getMessage(), containsString("does not exist"));
        assertFalse(indexExists(ParquetMounts.INDEX));
    }
}
