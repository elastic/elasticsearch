/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.Before;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Shared scaffold for the warm multi-file {@code COUNT(*)} dataset-aggregate regression suites
 * ({@link ExternalNdJsonManyFileWarmFoldIT}, {@link ExternalWarmDatasetAggregatePartialEvictionIT}):
 * the NDJSON/CSV fixture writers, the glob-URI builder, the single-row count assertion, the
 * parallel-parse pragmas, and the coordinator-pinned {@code run} override. Subclasses keep their own
 * file counts and node settings — the two suites deliberately sit at opposite ends of the cache-budget
 * spectrum (tiny LRU vs generous-plus-surgical-eviction).
 */
public abstract class AbstractWarmDatasetAggregateIT extends AbstractExternalDataSourceIT {

    /** Each file clears several 64kb segments so the parallel-parse / per-stripe fold path is exercised per file. */
    protected static final int FILE_BYTES = 512_000;

    /**
     * These suites drive the {@code parsing_parallelism} pragma to select the SEGMENTABLE_UNCOMPRESSED
     * parallel-parse regime the dataset-aggregate fold is built for. Pragmas are rejected on release
     * builds, so skip the whole suite there rather than run it in an unintended serial-parse shape — the
     * snapshot {@code internalClusterTest} run is the real coverage. Mirrors {@link AbstractPausableIntegTestCase}.
     */
    @Before
    public void requireQueryPragmas() {
        assumeTrue("requires the snapshot-only parsing_parallelism pragma", canUseQueryPragmas());
    }

    @Override
    protected QueryPragmas getPragmas() {
        // parsing_parallelism > 1 selects the SEGMENTABLE_UNCOMPRESSED parallel-parse path (the bench regime).
        return new QueryPragmas(Settings.builder().put("parsing_parallelism", 8).put("max_concurrent_open_segments", 2).build());
    }

    /**
     * The warm-stats cache is coordinator-local (per-node), so cold and warm MUST run on the same
     * coordinator or the warm lookup misses for a reason unrelated to the fold. Pin both to master
     * (mirrors {@link ExternalNdJsonMultiStripeFoldIT}) and apply the parallel-parse pragmas.
     */
    @Override
    public EsqlQueryResponse run(EsqlQueryRequest request, TimeValue timeout) {
        request.pragmas(getPragmas());
        return client(internalCluster().getMasterName()).execute(EsqlQueryAction.INSTANCE, request).actionGet(timeout);
    }

    protected static void assertCount(EsqlQueryResponse response, long expected) {
        List<List<Object>> rows = getValuesList(response);
        assertThat(rows.size(), equalTo(1));
        assertThat(((Number) rows.get(0).get(0)).longValue(), equalTo(expected));
    }

    protected static String globUri(Path dir, String pattern) {
        String dirUri = StoragePath.fileUri(dir).toString();
        if (dirUri.endsWith("/") == false) {
            dirUri += "/";
        }
        return dirUri + pattern;
    }

    protected static long writeNdjsonFile(Path file, long base) throws Exception {
        long rows = 0;
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            int written = 0;
            while (written < FILE_BYTES) {
                long a = base + rows;
                String line = "{\"a\":" + a + ",\"b\":" + (a * 10) + "}\n";
                w.write(line);
                written += line.length();
                rows++;
            }
        }
        return rows;
    }

    protected static long writeCsvFile(Path file, long base) throws Exception {
        long rows = 0;
        try (BufferedWriter w = Files.newBufferedWriter(file)) {
            String header = "a,b\n";
            w.write(header);
            int written = header.length();
            while (written < FILE_BYTES) {
                long a = base + rows;
                String line = a + "," + (a * 10) + "\n";
                w.write(line);
                written += line.length();
                rows++;
            }
        }
        return rows;
    }
}
