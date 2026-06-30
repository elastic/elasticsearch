/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.http.HttpDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.parquet.ParquetDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.AsyncExternalSourceOperator;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test verifying that {@code WHERE _file.modified > cutoff} prunes files at
 * listing time (via {@code GlobExpander.applyFileMetadataFilters}) so disqualified files
 * are never opened for reading by the async source operator.
 */
public class ExternalFileMetadataPruningIT extends AbstractEsqlIntegTestCase {

    private static final int ROWS_PER_FILE = 10;

    public static final class EsqlEnterpriseWithDatasourceExtensions extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        public void loadExtensions(ExtensiblePlugin.ExtensionLoader loader) {
            super.loadExtensions(loader);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class);
        plugins.add(EsqlEnterpriseWithDatasourceExtensions.class);
        plugins.add(HttpDataSourcePlugin.class);
        plugins.add(ParquetDataSourcePlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .putList(ExternalSourceSettings.LOCAL_ALLOWED_PATHS.getKey(), createTempDir().getParent().toString())
            .build();
    }

    @Before
    public void requireLocalFilesEnabled() {
        assumeTrue("requires local filesystem feature flag", HttpDataSourcePlugin.ESQL_EXTERNAL_DATASOURCES_LOCAL_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected QueryPragmas getPragmas() {
        return QueryPragmas.EMPTY;
    }

    /**
     * Writes 3 files with distinct lastModified timestamps; filters out the oldest;
     * asserts that only 2 files' rows were emitted by the source operator.
     */
    public void testFileModifiedFilterPrunesAtListingTime() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path dir = createTempDir();
        Path fileOld = writeParquetFile(dir, "old.parquet");
        Path fileMid = writeParquetFile(dir, "mid.parquet");
        Path fileNew = writeParquetFile(dir, "new.parquet");

        Instant oldTime = Instant.parse("2020-01-01T00:00:00Z");
        Instant midTime = Instant.parse("2023-06-15T00:00:00Z");
        Instant newTime = Instant.parse("2025-03-01T00:00:00Z");

        Files.setLastModifiedTime(fileOld, FileTime.from(oldTime));
        Files.setLastModifiedTime(fileMid, FileTime.from(midTime));
        Files.setLastModifiedTime(fileNew, FileTime.from(newTime));

        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        // Filter: _file.modified > 2022-01-01 → should include mid + new, exclude old
        String query = "EXTERNAL \""
            + dirUri
            + "*.parquet\" | WHERE `_file.modified` > \"2022-01-01T00:00:00.000Z\""
            + " | STATS c = COUNT(*)";

        var request = syncEsqlQueryRequest(query);
        request.profile(true);

        try (var response = run(request)) {
            List<List<Object>> rows = getValuesList(response);
            long count = ((Number) rows.get(0).get(0)).longValue();
            assertThat("Expected rows from mid + new files only", count, equalTo((long) ROWS_PER_FILE * 2));

            assertSourceOperatorRowsPruned(response, ROWS_PER_FILE * 2);
        }
    }

    /**
     * Asserts that the async external source operator emitted exactly {@code expectedRows},
     * proving that pruned files were never opened (not merely filtered post-read).
     */
    private static void assertSourceOperatorRowsPruned(EsqlQueryResponse response, long expectedRows) {
        var profile = response.profile();
        assertNotNull("profile must be present (request had profile=true)", profile);

        long totalRowsEmitted = 0;
        boolean foundAsyncOperator = false;
        for (var driver : profile.drivers()) {
            for (var op : driver.operators()) {
                if (op.status() instanceof AsyncExternalSourceOperator.Status status) {
                    foundAsyncOperator = true;
                    totalRowsEmitted += status.rowsEmitted();
                }
            }
        }
        assertTrue("expected at least one AsyncExternalSourceOperator in profile", foundAsyncOperator);
        assertThat("source operator should emit exactly the rows from non-pruned files", totalRowsEmitted, equalTo(expectedRows));
    }

    /**
     * Variant: filter excludes ALL files → resolver rejects with "no files" error,
     * proving that the listing-time prune removed all candidates before any read.
     */
    public void testFileModifiedFilterExcludesAllFiles() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path dir = createTempDir();
        Path fileA = writeParquetFile(dir, "a.parquet");
        Path fileB = writeParquetFile(dir, "b.parquet");

        Instant past = Instant.parse("2020-06-01T00:00:00Z");
        Files.setLastModifiedTime(fileA, FileTime.from(past));
        Files.setLastModifiedTime(fileB, FileTime.from(past));

        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        // All files are from 2020, filter requires > 2024 → nothing qualifies
        String query = "EXTERNAL \""
            + dirUri
            + "*.parquet\" | WHERE `_file.modified` > \"2024-01-01T00:00:00.000Z\""
            + " | STATS c = COUNT(*)";

        var request = syncEsqlQueryRequest(query);

        Exception e = expectThrows(Exception.class, () -> { run(request).close(); });
        assertThat(e.getMessage(), containsString("matched no files"));
    }

    /**
     * Variant: filter by _file.size prunes small files.
     */
    public void testFileSizeFilterPrunesAtListingTime() throws Exception {
        assumeTrue("requires EXTERNAL command capability", EXTERNAL_COMMAND.isEnabled());

        Path dir = createTempDir();
        Path fileSmall = writeParquetFile(dir, "small.parquet");
        Path fileBig = writeParquetFileWithRows(dir, "big.parquet", ROWS_PER_FILE * 5);

        long smallSize = Files.size(fileSmall);
        long bigSize = Files.size(fileBig);
        assertThat("big file must be larger than small file for the test to be meaningful", bigSize, greaterThan(smallSize));

        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        // Filter: only files larger than the small file's size
        String query = "EXTERNAL \"" + dirUri + "*.parquet\" | WHERE `_file.size` > " + smallSize + " | STATS c = COUNT(*)";

        var request = syncEsqlQueryRequest(query);
        request.profile(true);

        try (var response = run(request)) {
            List<List<Object>> rows = getValuesList(response);
            long count = ((Number) rows.get(0).get(0)).longValue();
            assertThat("Expected rows from big file only", count, equalTo((long) ROWS_PER_FILE * 5));

            assertSourceOperatorRowsPruned(response, ROWS_PER_FILE * 5);
        }
    }

    private Path writeParquetFile(Path dir, String filename) throws IOException {
        return writeParquetFileWithRows(dir, filename, ROWS_PER_FILE);
    }

    private Path writeParquetFileWithRows(Path dir, String filename, int rowCount) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType(
            "message test { required int64 id; required binary name (UTF8); required int32 value; }"
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = createOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withRowGroupSize(1024 * 1024)
                .build()
        ) {
            for (int i = 0; i < rowCount; i++) {
                Group g = factory.newGroup();
                g.add("id", (long) i);
                g.add("name", "row_" + i);
                g.add("value", i * 10);
                writer.write(g);
            }
        }

        Path filePath = dir.resolve(filename);
        Files.write(filePath, baos.toByteArray());
        return filePath;
    }

    private static OutputFile createOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return new PositionOutputStream() {
                    private long position = 0;

                    @Override
                    public long getPos() {
                        return position;
                    }

                    @Override
                    public void write(int b) throws IOException {
                        baos.write(b);
                        position++;
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        baos.write(b, off, len);
                        position += len;
                    }
                };
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }
}
