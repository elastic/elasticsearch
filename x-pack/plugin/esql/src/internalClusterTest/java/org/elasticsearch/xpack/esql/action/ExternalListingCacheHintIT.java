/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * End-to-end coverage for two listing-layer correctness defects, exercised through real text-format queries
 * ({@code WHERE} → hint extraction → glob rewrite → node-local listing cache), the chain the unit and resolver tests
 * inject the middle of. Run against CSV and NDJSON because the listing layer is shared by every text format — the
 * cache is one {@code ExternalSourceCacheService}, keyed identically whatever the reader.
 *
 * <p>Two conditions are load-bearing and easy to get subtly wrong:
 * <ul>
 *   <li><b>{@code schema_resolution: first_file_wins}.</b> The default is {@code union_by_name}, whose reconciliation
 *       path never consults the listing cache. A poisoning test on the default path passes for the wrong reason — it
 *       never touches the cache at all.</li>
 *   <li><b>Both queries on one coordinator.</b> The listing cache is a node singleton on the resolving coordinator, so
 *       the sequence must be pinned to a single node with {@code client(coordinator)} for the second query to hit the
 *       first's entry.</li>
 * </ul>
 *
 * <p>The glob has the shape {@code <root>/<key>=* / ** / *.<ext>} (without the spaces): the {@code key=*} segment is
 * what {@code GlobExpander.rewriteSegment} narrows, the double-star is required for {@code LocalStorageProvider} to
 * recurse, the concrete extension drives format inference, and folder values are left unpadded so the rewrite fires.
 */
public class ExternalListingCacheHintIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class, NdJsonDataSourcePlugin.class);
    }

    /** A text format under test: its file extension and how it materialises a two-column ({@code id,value}) file. */
    private enum TextFormat {
        CSV("csv", ".csv") {
            @Override
            String render(List<String[]> rows) {
                StringBuilder sb = new StringBuilder("id,value\n");
                for (String[] row : rows) {
                    sb.append(row[0]).append(',').append(row[1]).append('\n');
                }
                return sb.toString();
            }
        },
        NDJSON("ndjson", ".ndjson") {
            @Override
            String render(List<String[]> rows) {
                StringBuilder sb = new StringBuilder();
                for (String[] row : rows) {
                    sb.append("{\"id\":").append(row[0]).append(",\"value\":\"").append(row[1]).append("\"}\n");
                }
                return sb.toString();
            }
        };

        private final String tag;
        private final String ext;

        TextFormat(String tag, String ext) {
            this.tag = tag;
            this.ext = ext;
        }

        abstract String render(List<String[]> rows);
    }

    /**
     * The core defect (esql-planning#1174's headline example): a filtered query narrows the listing to a subset of the
     * files and caches it; keyed only on the path, that subset is then served to a later unfiltered query, which
     * silently reads fewer files than the dataset holds. Uses a {@code _file.name} filter on a plain glob — that
     * pruning runs on any multi-file listing, so the defect is not specific to hive-partitioned globs. The listing
     * cache is only consulted under {@code first_file_wins} (the default {@code union_by_name} never lists through it),
     * and it is a node singleton, so both queries are pinned to one coordinator.
     */
    public void testFilteredThenUnfilteredSeesEveryFile() throws Exception {
        for (TextFormat format : TextFormat.values()) {
            Path root = createTempDir().resolve("poison_" + format.tag);
            writeFile(root, "a", format, List.of(new String[] { "1", "alpha" }, new String[] { "2", "beta" }));
            writeFile(root, "b", format, List.of(new String[] { "3", "gamma" }, new String[] { "4", "delta" }));
            writeFile(root, "c", format, List.of(new String[] { "5", "epsilon" }, new String[] { "6", "zeta" }));

            String glob = StoragePath.fileUri(root) + "/*" + format.ext;
            String dataset = registerDataset("poison_" + format.tag, glob, Map.of("schema_resolution", "first_file_wins"));
            String coordinator = internalCluster().getNodeNames()[0];

            long filtered = count(
                coordinator,
                "FROM " + dataset + " METADATA _file.name | WHERE _file.name == \"a" + format.ext + "\" | STATS c = COUNT(*)"
            );
            assertEquals("[" + format + "] the filter keeps only a's rows", 2L, filtered);

            long unfiltered = count(coordinator, "FROM " + dataset + " | STATS c = COUNT(*)");
            assertEquals("[" + format + "] the unfiltered query must count every file, not the cached subset", 6L, unfiltered);
        }
    }

    /**
     * A filter that rewrites the glob onto a folder that does not exist must return zero rows, not raise "Glob pattern
     * matched no files". On the local filesystem the missing narrowed prefix throws in {@code listObjects}, so this
     * exercises the exception arm of the fallback specifically. Runs on the default (reconciliation) resolution, which
     * reads hive-partitioned rows and routes its listing through the same fallback.
     */
    public void testZeroMatchFilterReturnsZeroRowsNotError() throws Exception {
        for (TextFormat format : TextFormat.values()) {
            Path root = createTempDir().resolve("zeromatch_" + format.tag);
            writePartition(root.resolve("year=2024"), format, List.of(new String[] { "1", "alpha" }, new String[] { "2", "beta" }));

            String dataset = registerHivePartitioned("zeromatch_" + format.tag, root, "year", format);
            String coordinator = internalCluster().getNodeNames()[0];

            long count = count(coordinator, "FROM " + dataset + " | WHERE year == 2099 | STATS c = COUNT(*)");
            assertEquals("[" + format + "] an excluding filter returns zero rows, not an error", 0L, count);
        }
    }

    /**
     * The trap the fallback exists to avoid: {@code rewriteSegment} spells the value with {@code String.valueOf}, so
     * {@code WHERE month == 6} narrows the glob to {@code month=6} — but the folder on disk is the zero-padded
     * {@code month=06}. The rewritten glob matches nothing; without the fallback that is an error (or, if naively
     * "fixed" to return empty, silent zero rows). It must fall back to the full listing and return the matching rows.
     */
    public void testZeroPaddedMonthFolderAgainstUnpaddedPredicate() throws Exception {
        for (TextFormat format : TextFormat.values()) {
            Path root = createTempDir().resolve("padded_" + format.tag);
            writePartition(root.resolve("month=06"), format, List.of(new String[] { "1", "alpha" }, new String[] { "2", "beta" }));
            writePartition(root.resolve("month=07"), format, List.<String[]>of(new String[] { "3", "gamma" }));

            String dataset = registerHivePartitioned("padded_" + format.tag, root, "month", format);
            String coordinator = internalCluster().getNodeNames()[0];

            long count = count(coordinator, "FROM " + dataset + " | WHERE month == 6 | STATS c = COUNT(*)");
            assertEquals("[" + format + "] the zero-padded month=06 folder must still be read", 2L, count);
        }
    }

    /**
     * A comma-separated path where {@code WHERE month == 6} narrows segment A onto a zero-padded {@code month=06}
     * folder its rewrite misses, while segment B (an unpadded {@code month=6} folder) still matches. Because B keeps
     * the whole-path listing non-empty, the pre-fix whole-path fallback never fired and A's rows were silently
     * dropped — the query counted only B's rows. Per-segment fallback recovers A's. (Bug found in human review of
     * #153682.) Both segments carry the {@code month} partition so it stays a bound column over the union.
     */
    public void testCommaPathSegmentRewrittenToEmptyStillCounted() throws Exception {
        for (TextFormat format : TextFormat.values()) {
            Path base = createTempDir().resolve("comma_" + format.tag);
            Path a = base.resolve("a");
            Path b = base.resolve("b");
            writePartition(a.resolve("month=06"), format, List.of(new String[] { "1", "alpha" }, new String[] { "2", "beta" }));
            writePartition(
                b.resolve("month=6"),
                format,
                List.of(new String[] { "3", "gamma" }, new String[] { "4", "delta" }, new String[] { "5", "eps" })
            );

            @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
            String glob = StoragePath.fileUri(a)
                + "/month=*/**/*"
                + format.ext
                + ","
                + StoragePath.fileUri(b)
                + "/month=*/**/*"
                + format.ext;
            String dataset = registerDataset("comma_" + format.tag, glob, Map.of("hive_partitioning", true));
            String coordinator = internalCluster().getNodeNames()[0];

            long count = count(coordinator, "FROM " + dataset + " | WHERE month == 6 | STATS c = COUNT(*)");
            assertEquals("[" + format + "] segment a's zero-padded month=06 rows must be counted, not dropped", 5L, count);
        }
    }

    private String registerHivePartitioned(String name, Path root, String partitionKey, TextFormat format) {
        // The key=* segment is what the rewrite narrows; ** makes the local provider recurse. Left on the default
        // union_by_name resolution: this is a fallback (defect 2) test, which fires on the reconciliation path too and
        // avoids the unrelated first_file_wins + hive-partition virtual-column path.
        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/" + partitionKey + "=*/**/*" + format.ext;
        return registerDataset(name, glob, Map.of("hive_partitioning", true));
    }

    private long count(String coordinator, String query) {
        try (EsqlQueryResponse response = client(coordinator).execute(EsqlQueryAction.INSTANCE, syncEsqlQueryRequest(query)).actionGet()) {
            List<List<Object>> rows = getValuesList(response);
            return ((Number) rows.get(0).get(0)).longValue();
        }
    }

    /** Writes {@code <baseName>.<ext>} directly under {@code dir} — a flat, unpartitioned file. */
    private static void writeFile(Path dir, String baseName, TextFormat format, List<String[]> rows) throws Exception {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(baseName + format.ext), format.render(rows), StandardCharsets.UTF_8);
    }

    /** Writes {@code data.<ext>} under a Hive partition directory (e.g. {@code year=2024/}). */
    private static void writePartition(Path partitionDir, TextFormat format, List<String[]> rows) throws Exception {
        Files.createDirectories(partitionDir);
        Files.writeString(partitionDir.resolve("data" + format.ext), format.render(rows), StandardCharsets.UTF_8);
    }
}
