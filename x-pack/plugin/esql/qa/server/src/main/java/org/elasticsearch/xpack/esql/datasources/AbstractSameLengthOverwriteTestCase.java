/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.io.IOException;
import java.util.List;

/**
 * A Parquet object overwritten in place with a file of the same byte length must be read as the new file. The footer caches
 * are keyed on {@code (storage identity, path, length, version)}, so a query after the overwrite must not answer from the
 * footer of the first file: neither the statistics for {@code COUNT(*)}, {@code MIN} and {@code MAX}, nor the rows of a file
 * small enough for the footer prefetch. See elastic/esql-planning#2094.
 * <p>
 * Each case runs on a single file, whose version comes from a metadata probe, or through a glob, whose versions come from the
 * listing. Each overwrite waits past the listing TTL, which a subclass sets to {@link #SHORT_LISTING_TTL}, so the second query
 * sees the new version as it would once the TTL expires in production. Each case uses its own key and dataset, so the caches
 * of one case cannot answer for another.
 */
public abstract class AbstractSameLengthOverwriteTestCase extends AbstractExternalOverwriteRestTestCase {

    /** The listing TTL a subclass's cluster uses. Each overwrite waits a little longer than this. */
    protected static final String SHORT_LISTING_TTL = "1s";

    private static final String STATS = " | STATS c = COUNT(*), mn = MIN(v), mx = MAX(v)";
    private static final String FIRST_ROW = " | SORT v | KEEP v | LIMIT 1";

    public void testStatisticsOfASmallFile() throws Exception {
        assertNewFileAfterOverwrite(
            "stats_small",
            "stats_small.parquet",
            "stats_small.parquet",
            SMALL_V1,
            SMALL_V2,
            STATS,
            List.of(List.of(100L, 0L, 99L)),
            List.of(List.of(100L, 1_000_000L, 1_000_099L))
        );
    }

    public void testStatisticsOfALargeFile() throws Exception {
        assertNewFileAfterOverwrite(
            "stats_large",
            "stats_large.parquet",
            "stats_large.parquet",
            LARGE_V1,
            LARGE_V2,
            STATS,
            List.of(List.of(10_000L, 0L, 9_999L)),
            List.of(List.of(10_000L, 1_000_000L, 1_009_999L))
        );
    }

    public void testStatisticsOfALargeFileThroughAGlob() throws Exception {
        assertNewFileAfterOverwrite(
            "stats_glob",
            "stats_glob/*.parquet",
            "stats_glob/part-1.parquet",
            LARGE_V1,
            LARGE_V2,
            STATS,
            List.of(List.of(10_000L, 0L, 9_999L)),
            List.of(List.of(10_000L, 1_000_000L, 1_009_999L))
        );
    }

    public void testRowsOfASmallFile() throws Exception {
        assertNewFileAfterOverwrite(
            "rows",
            "rows.parquet",
            "rows.parquet",
            SMALL_V1,
            SMALL_V2,
            FIRST_ROW,
            List.of(List.of(0L)),
            List.of(List.of(1_000_000L))
        );
    }

    public void testRowsOfASmallFileThroughAGlob() throws Exception {
        assertNewFileAfterOverwrite(
            "rows_glob",
            "rows_glob/*.parquet",
            "rows_glob/part-1.parquet",
            SMALL_V1,
            SMALL_V2,
            FIRST_ROW,
            List.of(List.of(0L)),
            List.of(List.of(1_000_000L))
        );
    }

    /**
     * Control: an overwrite with a file one row longer is read as the new file. The length is part of the footer key, so this
     * case passes on a build without the fix as well.
     */
    public void testDifferentLengthOverwrite() throws Exception {
        assertNewFileAfterOverwrite(
            "longer",
            "longer.parquet",
            "longer.parquet",
            LARGE_V1,
            LONGER_V2,
            STATS,
            List.of(List.of(10_000L, 0L, 9_999L)),
            List.of(List.of(10_001L, 1_000_000L, 1_010_000L))
        );
    }

    /**
     * Registers a dataset over {@code resource}, writes {@code first} at {@code object}, and checks {@code tail} answers
     * {@code before}; then, past the listing TTL, overwrites the object with {@code second} and checks it answers {@code after}.
     */
    private void assertNewFileAfterOverwrite(
        String name,
        String resource,
        String object,
        String first,
        String second,
        String tail,
        List<List<Long>> before,
        List<List<Long>> after
    ) throws IOException {
        String dataset = "same_length_" + backend().name() + "_" + name;
        register(dataset, key(resource), null);
        upload(key(object), first);
        String query = "FROM " + dataset + tail;
        assertEquals(before, query(query));

        safeSleep(1_100);
        upload(key(object), second);
        assertEquals("the object was overwritten; the answer must come from the new file", after, query(query));
    }
}
