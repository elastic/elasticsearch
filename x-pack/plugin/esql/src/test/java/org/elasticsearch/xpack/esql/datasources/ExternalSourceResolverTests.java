/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.InputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link ExternalSourceResolver} multi-file schema resolution behavior.
 * <p>
 * Multi-file globs route through two distinct code paths inside
 * {@code resolveMultiFileSource}: a {@code FIRST_FILE_WINS} fast path that reads only the
 * lex-smallest anchor's metadata and pins it for every file, and a reconciliation path
 * (shared by {@code UNION_BY_NAME} and {@code STRICT}) that reads every file's metadata
 * up front and merges/validates schemas. Tests that exercise behavior invariant across
 * the two paths are parameterized over {@link #MULTI_FILE_STRATEGIES} so every CI run
 * walks both code paths; tests that lock down path-specific contracts (anchor schema
 * pinning, file count enrichment, stats-partial flag) stay path-scoped.
 */
public class ExternalSourceResolverTests extends ESTestCase {

    private static final int FILE_META_COUNT = FileMetadataColumns.COLUMNS.size();

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    // ===== FIRST_FILE_WINS tests (current behavior) =====

    /**
     * Multi-file glob with three files whose schemas widen across files: the anchor (file1) has
     * a strict subset of file2's columns, and file3 has a strict subset of file1's columns.
     * The two strategies must produce different but equally well-defined schemas:
     * <ul>
     *   <li>FFW pins the anchor's columns ([emp_no, name]); columns present only in non-anchor
     *       files (extra) are dropped, columns missing from non-anchor files are filled at read
     *       time.</li>
     *   <li>UNION_BY_NAME unions all columns in first-seen order ([emp_no, name, extra]); types
     *       are preserved verbatim since each column's type is consistent across files.</li>
     * </ul>
     */
    public void testMultiFileResolvedSchemaPerStrategy() throws Exception {
        List<Attribute> schema1 = List.of(attr("emp_no", DataType.INTEGER), attr("name", DataType.KEYWORD));
        List<Attribute> schema2 = List.of(attr("emp_no", DataType.INTEGER), attr("name", DataType.KEYWORD), attr("extra", DataType.LONG));
        List<Attribute> schema3 = List.of(attr("emp_no", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/file1.parquet", schema1);
        schemasByPath.put("s3://bucket/data/file2.parquet", schema2);
        schemasByPath.put("s3://bucket/data/file3.parquet", schema3);

        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/file1.parquet", 100),
            entry("s3://bucket/data/file2.parquet", 200),
            entry("s3://bucket/data/file3.parquet", 300)
        );

        Map<FormatReader.SchemaResolution, List<String>> expectedDataColumnNames = Map.of(
            FormatReader.SchemaResolution.FIRST_FILE_WINS,
            List.of("emp_no", "name"),
            FormatReader.SchemaResolution.UNION_BY_NAME,
            List.of("emp_no", "name", "extra")
        );

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            ExternalSourceResolution resolution = resolveMultiFileWithConfig(
                "s3://bucket/data/*.parquet",
                schemasByPath,
                listing,
                configFor(strategy)
            );

            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            List<String> expectedDataNames = expectedDataColumnNames.get(strategy);
            List<Attribute> resolvedSchema = resolved.metadata().schema();
            assertEquals("[" + strategy + "] resolved schema width", expectedDataNames.size() + FILE_META_COUNT, resolvedSchema.size());
            List<String> dataNames = resolvedSchema.stream().limit(expectedDataNames.size()).map(Attribute::name).toList();
            assertEquals("[" + strategy + "] resolved data column names", expectedDataNames, dataNames);
        }
    }

    /**
     * Same shape as {@link #testMultiFileResolvedSchemaPerStrategy} but with no column types
     * that could widen — under UBN, every union column keeps its original type. Locks in that
     * FFW drops extra non-anchor columns ({@code c:LONG}) while UBN preserves them in
     * first-seen order.
     */
    public void testMultiFileMismatchedSchemasPerStrategy() throws Exception {
        List<Attribute> schema1 = List.of(attr("a", DataType.KEYWORD), attr("b", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("a", DataType.KEYWORD), attr("b", DataType.INTEGER), attr("c", DataType.LONG));
        List<Attribute> schema3 = List.of(attr("a", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/f1.parquet", schema1);
        schemasByPath.put("s3://bucket/data/f2.parquet", schema2);
        schemasByPath.put("s3://bucket/data/f3.parquet", schema3);

        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/f1.parquet", 10),
            entry("s3://bucket/data/f2.parquet", 20),
            entry("s3://bucket/data/f3.parquet", 30)
        );

        Map<FormatReader.SchemaResolution, List<String>> expectedDataColumnNames = Map.of(
            FormatReader.SchemaResolution.FIRST_FILE_WINS,
            List.of("a", "b"),
            FormatReader.SchemaResolution.UNION_BY_NAME,
            List.of("a", "b", "c")
        );

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            ExternalSourceResolution resolution = resolveMultiFileWithConfig(
                "s3://bucket/data/*.parquet",
                schemasByPath,
                listing,
                configFor(strategy)
            );

            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            List<String> expectedDataNames = expectedDataColumnNames.get(strategy);
            List<Attribute> resolvedSchema = resolved.metadata().schema();
            assertEquals("[" + strategy + "] resolved schema width", expectedDataNames.size() + FILE_META_COUNT, resolvedSchema.size());
            List<String> dataNames = resolvedSchema.stream().limit(expectedDataNames.size()).map(Attribute::name).toList();
            assertEquals("[" + strategy + "] resolved data column names", expectedDataNames, dataNames);
        }
    }

    /**
     * Single-file glob expands to one entry. Both strategies must produce an identical
     * user-observable schema: the file's columns, in declaration order, with the same types.
     * The FFW path skips the multi-file stats-aggregation branch entirely; the UBN path runs
     * the reconciliation loop on a single-entry map and ends up unifying the schema with itself.
     */
    public void testSingleFileGlobSchemaInvariantAcrossStrategies() throws Exception {
        List<Attribute> schema = List.of(attr("id", DataType.LONG), attr("value", DataType.DOUBLE));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/only.parquet", schema);

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            ExternalSourceResolution resolution = resolveMultiFileWithConfig(
                "s3://bucket/data/*.parquet",
                schemasByPath,
                List.of(entry("s3://bucket/data/only.parquet", 500)),
                configFor(strategy)
            );

            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            List<Attribute> resolvedSchema = resolved.metadata().schema();
            assertEquals("[" + strategy + "] resolved schema width", 2 + FILE_META_COUNT, resolvedSchema.size());
            assertEquals("[" + strategy + "] resolved column 0 name", "id", resolvedSchema.get(0).name());
            assertEquals("[" + strategy + "] resolved column 1 name", "value", resolvedSchema.get(1).name());
            assertEquals("[" + strategy + "] resolved column 0 type", DataType.LONG, resolvedSchema.get(0).dataType());
            assertEquals("[" + strategy + "] resolved column 1 type", DataType.DOUBLE, resolvedSchema.get(1).dataType());
        }
    }

    // ===== Stats partial / file-count flag tests =====

    /**
     * Invariant: every schema-resolution mode marks stats as partial when at least one file
     * lacks statistics. {@code STATS_PARTIAL} is what tells downstream operators that aggregated
     * stats are incomplete and must not be trusted for shortcuts like {@code canSkipSplitDiscovery}.
     * Parameterized over {@link #MULTI_FILE_STRATEGIES} so any future {@code SchemaResolution}
     * value inherits the invariant by construction.
     */
    public void testMultiFileStatsPartialFlagPerStrategy() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);

        List<StorageEntry> listing = List.of(entry("s3://bucket/data/a.parquet", 100), entry("s3://bucket/data/b.parquet", 200));

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            ExternalSourceResolution resolution = resolveMultiFileWithConfig(
                "s3://bucket/data/*.parquet",
                schemasByPath,
                listing,
                configFor(strategy)
            );
            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            Object partial = resolved.metadata().sourceMetadata().get(SourceStatisticsSerializer.STATS_PARTIAL);
            assertEquals("[" + strategy + "] STATS_PARTIAL must be true when not every file reports statistics", Boolean.TRUE, partial);
        }
    }

    /**
     * Invariant: every schema-resolution mode stamps {@code STATS_FILE_COUNT} into the resolved
     * source metadata. {@code ComputeService#canSkipSplitDiscovery} reads this field to short-circuit
     * aggregate pushdown (COUNT/MIN/MAX) without scanning row groups; missing it forces Phase-2
     * split discovery to run even when the answer is in metadata. Parameterized over
     * {@link #MULTI_FILE_STRATEGIES} so any new mode inherits the invariant by construction.
     */
    public void testMultiFileFileCountPerStrategy() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);
        schemasByPath.put("s3://bucket/data/c.parquet", schema);

        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/a.parquet", 100),
            entry("s3://bucket/data/b.parquet", 200),
            entry("s3://bucket/data/c.parquet", 300)
        );

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            ExternalSourceResolution resolution = resolveMultiFileWithConfig(
                "s3://bucket/data/*.parquet",
                schemasByPath,
                listing,
                configFor(strategy)
            );
            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            Object fileCount = resolved.metadata().sourceMetadata().get(SourceStatisticsSerializer.STATS_FILE_COUNT);
            assertEquals("[" + strategy + "] STATS_FILE_COUNT must equal the number of discovered files", 3L, fileCount);
        }
    }

    /**
     * FFW resolution must populate schemaMap with one identity-mapped FileSchemaInfo entry per
     * discovered file, each carrying the anchor schema verbatim. Closest-layer assertion that the
     * planner's per-file pinning is wired correctly: this is what {@code FileSplitProvider} reads
     * to bake {@code FileSplit.readSchema} for every split, which in turn pins the reader.
     */
    public void testFirstFileWinsPopulatesSchemaMapForEveryFile() throws Exception {
        List<Attribute> anchorSchema = List.of(attr("col0", DataType.KEYWORD), attr("col1", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", anchorSchema);
        schemasByPath.put("s3://bucket/data/b.parquet", List.of(attr("col0", DataType.INTEGER), attr("col1", DataType.INTEGER)));
        schemasByPath.put("s3://bucket/data/c.parquet", List.of(attr("col0", DataType.INTEGER), attr("col1", DataType.KEYWORD)));

        ExternalSourceResolution resolution = resolveMultiFileWithConfig(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(
                entry("s3://bucket/data/a.parquet", 100),
                entry("s3://bucket/data/b.parquet", 200),
                entry("s3://bucket/data/c.parquet", 300)
            ),
            configFor(FormatReader.SchemaResolution.FIRST_FILE_WINS)
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = resolved.schemaMap();
        assertEquals("schemaMap must have one entry per matched file", 3, schemaMap.size());
        for (Map.Entry<StoragePath, SchemaReconciliation.FileSchemaInfo> e : schemaMap.entrySet()) {
            assertEquals(
                "every FFW per-file entry carries the anchor schema verbatim, regardless of the file's own inference",
                anchorSchema,
                e.getValue().fileSchema().attributes()
            );
            ColumnMapping mapping = e.getValue().mapping();
            assertNotNull("FFW entries carry an identity ColumnMapping", mapping);
            assertTrue("FFW per-file mapping is identity", mapping.isIdentity());
            assertEquals(
                "identity mapping matches anchor schema width",
                new ColumnMapping(identityIndex(anchorSchema.size()), null),
                mapping
            );
        }
    }

    /**
     * The schemaMap contract differs by code path and is asserted here under both:
     * <ul>
     *   <li>FFW: every entry's {@code fileSchema} is the anchor's schema <em>verbatim</em>
     *       (the planner pins the anchor down for every split), and the mapping is identity.</li>
     *   <li>UNION_BY_NAME: every entry's {@code fileSchema} is the file's own schema, and the
     *       mapping rewrites the unified schema into that file's local layout — including
     *       {@code -1} placeholders for columns the file is missing.</li>
     * </ul>
     * <p>
     * Schemas here are intentionally compatible (no widening conflicts) so the UBN path can
     * actually run end-to-end; type-conflict rejection is covered by SchemaReconciliationTests.
     */
    public void testMultiFileSchemaMapContractPerStrategy() throws Exception {
        List<Attribute> anchorSchema = List.of(attr("col0", DataType.KEYWORD), attr("col1", DataType.INTEGER));
        List<Attribute> schemaB = List.of(attr("col0", DataType.KEYWORD), attr("col1", DataType.INTEGER), attr("col2", DataType.LONG));
        List<Attribute> schemaC = List.of(attr("col0", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", anchorSchema);
        schemasByPath.put("s3://bucket/data/b.parquet", schemaB);
        schemasByPath.put("s3://bucket/data/c.parquet", schemaC);

        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/a.parquet", 100),
            entry("s3://bucket/data/b.parquet", 200),
            entry("s3://bucket/data/c.parquet", 300)
        );

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            ExternalSourceResolution resolution = resolveMultiFileWithConfig(
                "s3://bucket/data/*.parquet",
                schemasByPath,
                listing,
                configFor(strategy)
            );

            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = resolved.schemaMap();
            assertEquals("[" + strategy + "] schemaMap must have one entry per matched file", 3, schemaMap.size());

            if (strategy == FormatReader.SchemaResolution.FIRST_FILE_WINS) {
                // FFW pins the anchor schema down for every file with an identity mapping.
                for (Map.Entry<StoragePath, SchemaReconciliation.FileSchemaInfo> e : schemaMap.entrySet()) {
                    assertEquals(
                        "[FFW] " + e.getKey() + ": entry must carry the anchor schema verbatim",
                        anchorSchema,
                        e.getValue().fileSchema().attributes()
                    );
                    ColumnMapping mapping = e.getValue().mapping();
                    assertNotNull("[FFW] " + e.getKey() + ": ColumnMapping must be set", mapping);
                    assertEquals(
                        "[FFW] " + e.getKey() + ": identity mapping length matches anchor schema width",
                        anchorSchema.size(),
                        mapping.width()
                    );
                    for (int i = 0; i < mapping.width(); i++) {
                        assertEquals("[FFW] " + e.getKey() + ": localIndex(" + i + ") = " + i, i, mapping.localIndex(i));
                        assertNull("[FFW] " + e.getKey() + ": no casts at position " + i, mapping.cast(i));
                    }
                }
            } else {
                // UNION_BY_NAME: each entry's fileSchema is the file's own schema, and the
                // mapping rewrites the unified schema [col0, col1, col2] into the file's local
                // column order, with -1 for columns the file is missing. The metadata schema
                // is enriched with virtual file-metadata columns (_file.*) appended after the
                // data columns; assertions here cover the data prefix only.
                List<String> expectedDataColumns = List.of("col0", "col1", "col2");
                List<Attribute> unifiedSchema = resolved.metadata().schema();
                assertEquals("[" + strategy + "] unified schema width", expectedDataColumns.size() + FILE_META_COUNT, unifiedSchema.size());
                List<String> dataColumnNames = unifiedSchema.stream().limit(expectedDataColumns.size()).map(Attribute::name).toList();
                assertEquals("[" + strategy + "] unified data columns", expectedDataColumns, dataColumnNames);

                Map<String, int[]> expectedLocalIndices = Map.of(
                    "s3://bucket/data/a.parquet",
                    new int[] { 0, 1, -1 },
                    "s3://bucket/data/b.parquet",
                    new int[] { 0, 1, 2 },
                    "s3://bucket/data/c.parquet",
                    new int[] { 0, -1, -1 }
                );
                Map<String, List<Attribute>> expectedFileSchemas = Map.of(
                    "s3://bucket/data/a.parquet",
                    anchorSchema,
                    "s3://bucket/data/b.parquet",
                    schemaB,
                    "s3://bucket/data/c.parquet",
                    schemaC
                );

                for (Map.Entry<StoragePath, SchemaReconciliation.FileSchemaInfo> e : schemaMap.entrySet()) {
                    String pathStr = e.getKey().toString();
                    assertEquals(
                        "[" + strategy + "] " + pathStr + ": fileSchema must equal the file's own schema",
                        expectedFileSchemas.get(pathStr),
                        e.getValue().fileSchema().attributes()
                    );
                    ColumnMapping mapping = e.getValue().mapping();
                    assertNotNull("[" + strategy + "] " + pathStr + ": ColumnMapping must be set", mapping);
                    int[] expected = expectedLocalIndices.get(pathStr);
                    // Mapping covers data columns only; virtual file-metadata columns are added
                    // post-read via VirtualColumnIterator and are not part of the per-file mapping.
                    assertEquals(
                        "[" + strategy + "] " + pathStr + ": mapping width = unified data column count",
                        expectedDataColumns.size(),
                        mapping.width()
                    );
                    for (int i = 0; i < mapping.width(); i++) {
                        assertEquals("[" + strategy + "] " + pathStr + ": localIndex(" + i + ")", expected[i], mapping.localIndex(i));
                        // No type drift in this fixture → no casts under UBN.
                        assertNull("[" + strategy + "] " + pathStr + ": no casts at position " + i, mapping.cast(i));
                    }
                }
            }
        }
    }

    /**
     * A single-file glob match must never set STATS_PARTIAL: there are no other files whose
     * statistics could be missing. Holds under both code paths — FFW skips the
     * multi-file stats branch entirely; UBN's reconciliation aggregates the single file's
     * (empty) stats and leaves the flag absent.
     */
    public void testSingleFileGlobDoesNotSetStatsPartialAcrossStrategies() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/only.parquet", schema);

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            ExternalSourceResolution resolution = resolveMultiFileWithConfig(
                "s3://bucket/data/*.parquet",
                schemasByPath,
                List.of(entry("s3://bucket/data/only.parquet", 100)),
                configFor(strategy)
            );

            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            assertNull(
                "[" + strategy + "] STATS_PARTIAL must be absent for single-file matches",
                resolved.metadata().sourceMetadata().get(SourceStatisticsSerializer.STATS_PARTIAL)
            );
        }
    }

    /**
     * When every file provides per-file row counts, every code path must produce the same
     * aggregated row count (sum across files), must not flag the stats as partial, and must
     * stamp {@code STATS_FILE_COUNT}. The cross-mode {@code STATS_FILE_COUNT} invariant is
     * pinned separately by {@link #testMultiFileFileCountPerStrategy}; here we assert all
     * three (row count + not-partial + file count) together for the stats-available case.
     */
    public void testMultiFileAggregatesRowCountAcrossStrategiesWhenStatsAvailable() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);
        schemasByPath.put("s3://bucket/data/c.parquet", schema);

        Map<String, Long> rowCountsByPath = new HashMap<>();
        rowCountsByPath.put("s3://bucket/data/a.parquet", 1000L);
        rowCountsByPath.put("s3://bucket/data/b.parquet", 2000L);
        rowCountsByPath.put("s3://bucket/data/c.parquet", 3000L);

        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/a.parquet", 100),
            entry("s3://bucket/data/b.parquet", 200),
            entry("s3://bucket/data/c.parquet", 300)
        );

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            ExternalSourceResolution resolution = resolveMultiFileWithStats(
                "s3://bucket/data/*.parquet",
                schemasByPath,
                rowCountsByPath,
                listing,
                configFor(strategy)
            );

            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            Map<String, Object> meta = resolved.metadata().sourceMetadata();
            assertEquals("[" + strategy + "] aggregated row count", 6000L, meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
            assertNull(
                "[" + strategy + "] STATS_PARTIAL must be absent when every file has stats",
                meta.get(SourceStatisticsSerializer.STATS_PARTIAL)
            );
            assertEquals(
                "[" + strategy + "] enrichWithFileCount must populate STATS_FILE_COUNT",
                3L,
                meta.get(SourceStatisticsSerializer.STATS_FILE_COUNT)
            );
        }
    }

    // ===== GenericFileList threading tests =====

    public void testMultiFileResolutionReturnsGenericFileList() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);

        List<StorageEntry> entries = List.of(entry("s3://bucket/data/a.parquet", 100), entry("s3://bucket/data/b.parquet", 200));

        ExternalSourceResolution resolution = resolveMultiFile("s3://bucket/data/*.parquet", schemasByPath, entries);

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        FileList fileList = resolved.fileList();
        assertTrue(fileList.isResolved());
        assertEquals(2, fileList.fileCount());
        assertEquals("s3://bucket/data/a.parquet", fileList.path(0).toString());
        assertEquals("s3://bucket/data/b.parquet", fileList.path(1).toString());
    }

    public void testMultiFileResolutionPreservesOriginalPattern() throws Exception {
        List<Attribute> schema = List.of(attr("col", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/dir/x.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/dir/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/dir/x.parquet", 50))
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/dir/*.parquet");
        assertNotNull(resolved);
        assertEquals("s3://bucket/dir/*.parquet", resolved.fileList().originalPattern());
    }

    public void testGlobNoMatchThrows() {
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();

        Exception e = expectThrows(RuntimeException.class, () -> resolveMultiFile("s3://bucket/data/*.parquet", schemasByPath, List.of()));
        assertTrue(e.getMessage().contains("Glob pattern matched no files"));
    }

    // ===== Single-file resolution returns a resolved singleton FileList =====

    public void testSingleFileResolutionReturnsResolvedSingletonFileList() throws Exception {
        List<Attribute> schema = List.of(attr("id", DataType.LONG));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/single.parquet", schema);

        ExternalSourceResolution resolution = resolveSingleFile("s3://bucket/data/single.parquet", schemasByPath);

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/single.parquet");
        assertNotNull(resolved);
        FileList fileList = resolved.fileList();
        assertTrue(fileList.isResolved());
        assertEquals(1, fileList.fileCount());
        assertEquals("s3://bucket/data/single.parquet", fileList.path(0).toString());
        assertEquals(0L, fileList.size(0));
    }

    /**
     * Single-file resolution must populate a one-entry schemaMap with the metadata schema and an
     * identity ColumnMapping, mirroring the multi-file FFW case. Closest-layer assertion that the
     * single-file path is not an elision — downstream readers honor readSchema uniformly across
     * single-file and multi-file queries.
     */
    public void testSingleFileResolutionPopulatesSchemaMap() throws Exception {
        List<Attribute> schema = List.of(attr("col0", DataType.KEYWORD), attr("col1", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/single.parquet", schema);

        ExternalSourceResolution resolution = resolveSingleFile("s3://bucket/data/single.parquet", schemasByPath);

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/single.parquet");
        assertNotNull(resolved);
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = resolved.schemaMap();
        assertEquals("single-file schemaMap must have exactly one entry", 1, schemaMap.size());
        SchemaReconciliation.FileSchemaInfo info = schemaMap.values().iterator().next();
        assertEquals("fileSchema must equal metadata schema verbatim", schema, info.fileSchema().attributes());
        ColumnMapping mapping = info.mapping();
        assertNotNull("single-file entry carries an identity ColumnMapping", mapping);
        assertTrue("single-file mapping is identity", mapping.isIdentity());
        assertEquals("identity mapping matches schema width", new ColumnMapping(identityIndex(schema.size()), null), mapping);
    }

    // ===== ExternalSchema type preservation =====

    public void testSchemaTypesPreserved() throws Exception {
        List<Attribute> schema = List.of(
            attr("id", DataType.LONG),
            attr("name", DataType.KEYWORD),
            attr("score", DataType.DOUBLE),
            attr("active", DataType.BOOLEAN),
            attr("count", DataType.INTEGER)
        );

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/typed.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/data/typed.parquet", 100))
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        List<Attribute> resolvedSchema = resolved.metadata().schema();
        assertEquals(5 + FILE_META_COUNT, resolvedSchema.size());
        assertEquals(DataType.LONG, resolvedSchema.get(0).dataType());
        assertEquals(DataType.KEYWORD, resolvedSchema.get(1).dataType());
        assertEquals(DataType.DOUBLE, resolvedSchema.get(2).dataType());
        assertEquals(DataType.BOOLEAN, resolvedSchema.get(3).dataType());
        assertEquals(DataType.INTEGER, resolvedSchema.get(4).dataType());
    }

    // ===== Default schema resolution strategy =====

    /**
     * Both the SPI default ({@link FormatReader#defaultSchemaResolution()}) and the resolver's
     * config-parse fallback ({@code parseSchemaResolution(null/missing)}) must derive from the
     * same constant — keeping them in lockstep is the whole point of
     * {@link FormatReader#DEFAULT_SCHEMA_RESOLUTION}. This test catches a drift between the two
     * (which previously had to be kept in sync by convention).
     */
    public void testDefaultSchemaResolutionIsSingleSourceOfTruth() {
        FormatReader reader = new StubFormatReader(Map.of());
        assertEquals(
            "SPI default must equal the FormatReader.DEFAULT_SCHEMA_RESOLUTION constant",
            FormatReader.DEFAULT_SCHEMA_RESOLUTION,
            reader.defaultSchemaResolution()
        );
        assertEquals(
            "Resolver's null-config fallback must equal the FormatReader.DEFAULT_SCHEMA_RESOLUTION constant",
            FormatReader.DEFAULT_SCHEMA_RESOLUTION,
            ExternalSourceResolver.parseSchemaResolution(null)
        );
        assertEquals(
            "Resolver's missing-key fallback must equal the FormatReader.DEFAULT_SCHEMA_RESOLUTION constant",
            FormatReader.DEFAULT_SCHEMA_RESOLUTION,
            ExternalSourceResolver.parseSchemaResolution(Map.of())
        );
    }

    // ===== Multiple paths resolution =====

    public void testMultiplePathsResolvedIndependently() throws Exception {
        List<Attribute> schema1 = List.of(attr("a", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("b", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/dir1/f1.parquet", schema1);
        schemasByPath.put("s3://bucket/dir2/f2.parquet", schema2);

        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        listingsByPrefix.put("s3://bucket/dir1/", List.of(entry("s3://bucket/dir1/f1.parquet", 100)));
        listingsByPrefix.put("s3://bucket/dir2/", List.of(entry("s3://bucket/dir2/f2.parquet", 200)));

        ExternalSourceResolution resolution = resolveMultiplePaths(
            List.of("s3://bucket/dir1/*.parquet", "s3://bucket/dir2/*.parquet"),
            schemasByPath,
            listingsByPrefix
        );

        ExternalSourceResolution.ResolvedSource resolved1 = resolution.resolvedSource("s3://bucket/dir1/*.parquet");
        assertNotNull(resolved1);
        assertEquals("a", resolved1.metadata().schema().get(0).name());

        ExternalSourceResolution.ResolvedSource resolved2 = resolution.resolvedSource("s3://bucket/dir2/*.parquet");
        assertNotNull(resolved2);
        assertEquals("b", resolved2.metadata().schema().get(0).name());
    }

    // ===== Config passthrough =====

    public void testConfigPassedThroughToMetadata() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/f.parquet", schema);

        Map<String, Object> config = Map.of("access_key", "test-key", "secret_key", "test-secret");

        ExternalSourceResolution resolution = resolveMultiFileWithConfig(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/data/f.parquet", 100)),
            config
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        assertEquals("test-key", resolved.metadata().config().get("access_key"));
        assertEquals("test-secret", resolved.metadata().config().get("secret_key"));
    }

    // ===== Partition column enrichment =====

    public void testPartitionColumnsAppendedAtTail() throws Exception {
        List<Attribute> schema = List.of(attr("emp_no", DataType.INTEGER), attr("name", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/year=2024/file1.parquet", schema);
        schemasByPath.put("s3://bucket/data/year=2023/file2.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/year=*/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/data/year=2024/file1.parquet", 100), entry("s3://bucket/data/year=2023/file2.parquet", 200))
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/year=*/*.parquet");
        assertNotNull(resolved);
        List<Attribute> resolvedSchema = resolved.metadata().schema();
        assertEquals(3 + FILE_META_COUNT, resolvedSchema.size());
        assertEquals("emp_no", resolvedSchema.get(0).name());
        assertEquals("name", resolvedSchema.get(1).name());
        assertEquals("year", resolvedSchema.get(2).name());
        assertEquals(DataType.INTEGER, resolvedSchema.get(2).dataType());
    }

    public void testPartitionColumnConflictPartitionWins() throws Exception {
        List<Attribute> schema = List.of(attr("year", DataType.KEYWORD), attr("name", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/year=2024/file1.parquet", schema);
        schemasByPath.put("s3://bucket/data/year=2023/file2.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/year=*/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/data/year=2024/file1.parquet", 100), entry("s3://bucket/data/year=2023/file2.parquet", 200))
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/year=*/*.parquet");
        assertNotNull(resolved);
        List<Attribute> resolvedSchema = resolved.metadata().schema();
        assertEquals(2 + FILE_META_COUNT, resolvedSchema.size());
        assertEquals("name", resolvedSchema.get(0).name());
        assertEquals("year", resolvedSchema.get(1).name());
        // Partition column type should be INTEGER (from path), not KEYWORD (from data)
        assertEquals(DataType.INTEGER, resolvedSchema.get(1).dataType());
    }

    public void testNoPartitionsSchemaUnchanged() throws Exception {
        List<Attribute> schema = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/file1.parquet", schema);
        schemasByPath.put("s3://bucket/data/file2.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/data/file1.parquet", 100), entry("s3://bucket/data/file2.parquet", 200))
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        List<Attribute> resolvedSchema = resolved.metadata().schema();
        assertEquals(2 + FILE_META_COUNT, resolvedSchema.size());
        assertEquals("a", resolvedSchema.get(0).name());
        assertEquals("b", resolvedSchema.get(1).name());
    }

    public void testMultiplePartitionColumns() throws Exception {
        List<Attribute> schema = List.of(attr("value", DataType.DOUBLE));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/year=2024/month=01/file.parquet", schema);
        schemasByPath.put("s3://bucket/data/year=2023/month=12/file.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/year=*/month=*/*.parquet",
            schemasByPath,
            List.of(
                entry("s3://bucket/data/year=2024/month=01/file.parquet", 100),
                entry("s3://bucket/data/year=2023/month=12/file.parquet", 200)
            )
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/year=*/month=*/*.parquet");
        assertNotNull(resolved);
        List<Attribute> resolvedSchema = resolved.metadata().schema();
        assertEquals(3 + FILE_META_COUNT, resolvedSchema.size());
        // Data column is first
        assertEquals("value", resolvedSchema.get(0).name());
        // Partition columns appended at tail in path declaration order
        assertEquals("year", resolvedSchema.get(1).name());
        assertEquals("month", resolvedSchema.get(2).name());
        assertThat(resolvedSchema.get(1), instanceOf(ReferenceAttribute.class));
        assertThat(resolvedSchema.get(2), instanceOf(ReferenceAttribute.class));
        // End-to-end check: HivePartitionDetector produced non-null values for every file, so the
        // resolver emits Nullability.FALSE for both partition columns.
        assertEquals(Nullability.FALSE, resolvedSchema.get(1).nullable());
        assertEquals(Nullability.FALSE, resolvedSchema.get(2).nullable());
    }

    public void testEnrichSchemaWithPartitionColumnsDirectly() {
        List<Attribute> originalSchema = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));
        ExternalSourceMetadata metadata = createStubMetadata("s3://bucket/file.parquet", originalSchema);

        LinkedHashMap<String, DataType> partCols = new LinkedHashMap<>();
        partCols.put("year", DataType.INTEGER);
        partCols.put("region", DataType.KEYWORD);
        PartitionMetadata partitions = new PartitionMetadata(partCols, Map.of());

        ExternalSourceMetadata enriched = ExternalSourceResolver.enrichSchemaWithPartitionColumns(metadata, partitions);
        List<Attribute> schema = enriched.schema();
        assertEquals(4, schema.size());
        assertEquals("a", schema.get(0).name());
        assertEquals("b", schema.get(1).name());
        // Partition columns appended at tail in declaration order
        assertEquals("year", schema.get(2).name());
        assertEquals("region", schema.get(3).name());
        // Partition columns are ReferenceAttributes, not FieldAttributes
        assertThat(schema.get(2), instanceOf(ReferenceAttribute.class));
        assertThat(schema.get(3), instanceOf(ReferenceAttribute.class));
        // Partition columns are user-addressable, so they must NOT be synthetic; otherwise
        // AnalyzerRules.maybeResolveAgainstList skips them during name resolution.
        assertFalse(schema.get(2).synthetic());
        assertFalse(schema.get(3).synthetic());
        // No per-file evidence is supplied here: every partition column must stay Nullability.TRUE.
        assertEquals(Nullability.TRUE, schema.get(2).nullable());
        assertEquals(Nullability.TRUE, schema.get(3).nullable());
    }

    public void testEnrichSchemaWithPartitionColumnsEmitsNullabilityFalseWhenNoNulls() {
        // Per-query optimization: when every matched file has a non-null value for the partition
        // column, the resolver emits Nullability.FALSE so downstream rules that consult nullability
        // (Coalesce simplification, PropagateNullable) have correct metadata.
        List<Attribute> originalSchema = List.of(attr("value", DataType.DOUBLE));
        ExternalSourceMetadata metadata = createStubMetadata("s3://bucket/data/*.parquet", originalSchema);

        LinkedHashMap<String, DataType> partCols = new LinkedHashMap<>();
        partCols.put("year", DataType.INTEGER);
        partCols.put("month", DataType.INTEGER);

        Map<StoragePath, Map<String, Object>> filePartitions = new LinkedHashMap<>();
        filePartitions.put(StoragePath.of("s3://bucket/data/year=2024/month=01/f1.parquet"), Map.of("year", 2024, "month", 1));
        filePartitions.put(StoragePath.of("s3://bucket/data/year=2024/month=02/f2.parquet"), Map.of("year", 2024, "month", 2));
        PartitionMetadata partitions = new PartitionMetadata(partCols, filePartitions);

        ExternalSourceMetadata enriched = ExternalSourceResolver.enrichSchemaWithPartitionColumns(metadata, partitions);
        List<Attribute> schema = enriched.schema();
        assertEquals(3, schema.size());
        assertEquals("year", schema.get(1).name());
        assertEquals("month", schema.get(2).name());
        assertEquals(Nullability.FALSE, schema.get(1).nullable());
        assertEquals(Nullability.FALSE, schema.get(2).nullable());
    }

    public void testEnrichSchemaWithPartitionColumnsEmitsNullabilityTrueForHiveDefaultSentinel() {
        // When at least one file lives under __HIVE_DEFAULT_PARTITION__ (decoded to null in
        // PartitionMetadata#filePartitionValues by HivePartitionDetector), the resolver must keep
        // Nullability.TRUE for that column. Sibling partition columns that are still all-non-null
        // remain Nullability.FALSE.
        List<Attribute> originalSchema = List.of(attr("value", DataType.DOUBLE));
        ExternalSourceMetadata metadata = createStubMetadata("s3://bucket/data/*.parquet", originalSchema);

        LinkedHashMap<String, DataType> partCols = new LinkedHashMap<>();
        partCols.put("year", DataType.INTEGER);
        partCols.put("month", DataType.INTEGER);

        Map<StoragePath, Map<String, Object>> filePartitions = new LinkedHashMap<>();
        filePartitions.put(StoragePath.of("s3://bucket/data/year=2024/month=01/f1.parquet"), Map.of("year", 2024, "month", 1));
        Map<String, Object> nullMonth = new HashMap<>();
        nullMonth.put("year", 2024);
        nullMonth.put("month", null);
        filePartitions.put(StoragePath.of("s3://bucket/data/year=2024/month=__HIVE_DEFAULT_PARTITION__/f2.parquet"), nullMonth);
        PartitionMetadata partitions = new PartitionMetadata(partCols, filePartitions);

        ExternalSourceMetadata enriched = ExternalSourceResolver.enrichSchemaWithPartitionColumns(metadata, partitions);
        List<Attribute> schema = enriched.schema();
        assertEquals(3, schema.size());
        assertEquals("year", schema.get(1).name());
        assertEquals("month", schema.get(2).name());
        // year has no nulls in the matched fileset → provably non-null.
        assertEquals(Nullability.FALSE, schema.get(1).nullable());
        // month contains a sentinel-decoded null → must stay nullable.
        assertEquals(Nullability.TRUE, schema.get(2).nullable());
    }

    public void testSchemaWithFieldAttributeFailsValidation() throws Exception {
        List<Attribute> schemaWithFieldAttr = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/file.parquet", schemaWithFieldAttr);

        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        listingsByPrefix.put("s3://bucket/data/", List.of(entry("s3://bucket/data/file.parquet", 100)));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> resolveMultiplePaths(List.of("s3://bucket/data/file.parquet"), schemasByPath, listingsByPrefix)
        );
        assertThat(e.getMessage(), containsString("ReferenceAttribute"));
        assertThat(e.getMessage(), containsString("FieldAttribute"));
    }

    private ExternalSourceMetadata createStubMetadata(String location, List<Attribute> schema) {
        return new ExternalSourceMetadata() {
            @Override
            public String location() {
                return location;
            }

            @Override
            public List<Attribute> schema() {
                return schema;
            }

            @Override
            public String sourceType() {
                return "parquet";
            }
        };
    }

    // ===== Empty resolution =====

    public void testEmptyPathListReturnsEmptyResolution() throws Exception {
        ExternalSourceResolver resolver = createResolver(Map.of(), Map.of());
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(List.of(), Map.of(), future);
        ExternalSourceResolution resolution = future.actionGet();
        assertTrue(resolution.isEmpty());
    }

    public void testNullPathListReturnsEmptyResolution() throws Exception {
        ExternalSourceResolver resolver = createResolver(Map.of(), Map.of());
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(null, Map.of(), future);
        ExternalSourceResolution resolution = future.actionGet();
        assertTrue(resolution.isEmpty());
    }

    // ===== Resolver + Cache integration =====

    public void testCacheReducesListingAndSchemaLoaderCalls() throws Exception {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);

        List<StorageEntry> listing = List.of(entry("s3://bucket/data/a.parquet", 100), entry("s3://bucket/data/b.parquet", 200));

        CountingStorageProvider countingProvider = new CountingStorageProvider(Map.of("s3://bucket/data/", listing), schemasByPath);

        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();

        // FFW-specific assertions of listing-cache + anchor-schema-cache reuse.
        // The cross-mode cache invariant (every SchemaResolution strategy hits the cache on
        // warm resolves) is covered separately by testMultiFileCacheReducesSchemaLoaderCallsPerStrategy.
        Map<String, Map<String, Object>> pathConfigs = Map.of(
            "s3://bucket/data/*.parquet",
            new HashMap<>(configFor(FormatReader.SchemaResolution.FIRST_FILE_WINS))
        );
        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(settings)) {
            ExternalSourceResolver resolver = createResolverWithCache(countingProvider, schemasByPath, cacheService);

            PlainActionFuture<ExternalSourceResolution> f1 = new PlainActionFuture<>();
            resolver.resolve(List.of("s3://bucket/data/*.parquet"), pathConfigs, f1);
            ExternalSourceResolution res1 = f1.actionGet();
            assertNotNull(res1.resolvedSource("s3://bucket/data/*.parquet"));
            assertEquals(2, res1.resolvedSource("s3://bucket/data/*.parquet").fileList().fileCount());
            int listCallsAfterFirst = countingProvider.listCallCount.get();
            int schemaCallsAfterFirst = countingProvider.schemaCallCount.get();
            assertTrue("listing loader should have been called at least once", listCallsAfterFirst > 0);
            assertTrue("schema loader should have been called at least once", schemaCallsAfterFirst > 0);

            PlainActionFuture<ExternalSourceResolution> f2 = new PlainActionFuture<>();
            resolver.resolve(List.of("s3://bucket/data/*.parquet"), pathConfigs, f2);
            ExternalSourceResolution res2 = f2.actionGet();
            assertNotNull(res2.resolvedSource("s3://bucket/data/*.parquet"));
            assertEquals(2, res2.resolvedSource("s3://bucket/data/*.parquet").fileList().fileCount());

            assertEquals(
                "listing loader should not be called again on cache hit",
                listCallsAfterFirst,
                countingProvider.listCallCount.get()
            );
            assertEquals(
                "schema loader should not be called again on cache hit",
                schemaCallsAfterFirst,
                countingProvider.schemaCallCount.get()
            );
        }
    }

    /**
     * Invariant: every schema-resolution mode must consult the schema cache on the per-file
     * resolve. A second resolve of the same glob across the same paths must add zero schema-loader
     * calls. Parameterized over {@link #MULTI_FILE_STRATEGIES} so any new mode inherits the
     * invariant by construction; the bug fixed by this PR (UNION_BY_NAME default flip in
     * elastic/elasticsearch#149176) was that the reconciliation path bypassed the schema cache,
     * so every warm multi-file query re-read N footers from storage.
     */
    public void testMultiFileCacheReducesSchemaLoaderCallsPerStrategy() throws Exception {
        Settings cacheSettings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();

        for (FormatReader.SchemaResolution strategy : MULTI_FILE_STRATEGIES) {
            List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
            Map<String, List<Attribute>> schemasByPath = new HashMap<>();
            schemasByPath.put("s3://bucket/data/a.parquet", schema);
            schemasByPath.put("s3://bucket/data/b.parquet", schema);
            schemasByPath.put("s3://bucket/data/c.parquet", schema);

            List<StorageEntry> listing = List.of(
                entry("s3://bucket/data/a.parquet", 100),
                entry("s3://bucket/data/b.parquet", 200),
                entry("s3://bucket/data/c.parquet", 300)
            );

            CountingStorageProvider countingProvider = new CountingStorageProvider(Map.of("s3://bucket/data/", listing), schemasByPath);

            Map<String, Map<String, Object>> pathConfigs = Map.of("s3://bucket/data/*.parquet", new HashMap<>(configFor(strategy)));

            try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(cacheSettings)) {
                ExternalSourceResolver resolver = createResolverWithCache(countingProvider, schemasByPath, cacheService);

                PlainActionFuture<ExternalSourceResolution> f1 = new PlainActionFuture<>();
                resolver.resolve(List.of("s3://bucket/data/*.parquet"), pathConfigs, f1);
                ExternalSourceResolution res1 = f1.actionGet();
                assertNotNull("[" + strategy + "] first resolve must produce a source", res1.resolvedSource("s3://bucket/data/*.parquet"));
                int schemaCallsAfterFirst = countingProvider.schemaCallCount.get();
                assertTrue("[" + strategy + "] schema loader must be invoked on first resolve", schemaCallsAfterFirst > 0);

                PlainActionFuture<ExternalSourceResolution> f2 = new PlainActionFuture<>();
                resolver.resolve(List.of("s3://bucket/data/*.parquet"), pathConfigs, f2);
                ExternalSourceResolution res2 = f2.actionGet();
                assertNotNull("[" + strategy + "] second resolve must produce a source", res2.resolvedSource("s3://bucket/data/*.parquet"));

                assertEquals(
                    "[" + strategy + "] schema loader must not be called again on second resolve (cache hit invariant)",
                    schemaCallsAfterFirst,
                    countingProvider.schemaCallCount.get()
                );
            }
        }
    }

    public void testAggregateFileStatisticsAcceptsCachedAndUncachedShapes() {
        long uncachedRowCount = 42L;
        long cachedRowCount = 58L;

        SourceMetadata uncached = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return List.of();
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/uncached.parquet";
            }

            @Override
            public Optional<SourceStatistics> statistics() {
                return Optional.of(statsOf(uncachedRowCount));
            }
        };

        Map<String, Object> cachedFlatStats = SourceStatisticsSerializer.embedStatistics(Map.of(), statsOf(cachedRowCount));
        SourceMetadata cached = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return List.of();
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/cached.parquet";
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return cachedFlatStats;
            }
        };

        Map<String, Object> merged = ExternalSourceResolver.aggregateFileStatistics(List.of(uncached, cached));
        assertNotNull(merged);
        assertEquals(uncachedRowCount + cachedRowCount, ((Number) merged.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue());

        SourceMetadata missing = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return List.of();
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/missing.parquet";
            }
        };
        assertNull(ExternalSourceResolver.aggregateFileStatistics(List.of(uncached, cached, missing)));
    }

    private static SourceStatistics statsOf(long rowCount) {
        return new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(rowCount);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.empty();
            }
        };
    }

    public void testAggregateFileStatisticsMergesColumnStatsAcrossShapes() {
        String col = "eventDate";
        long uncachedRowCount = 100L;
        long cachedRowCount = 200L;
        long uncachedNullCount = 5L;
        long cachedNullCount = 3L;
        long uncachedMin = 10L;
        long uncachedMax = 100L;
        long cachedMin = 50L;
        long cachedMax = 200L;

        SourceStatistics uncachedStats = statsWithColumn(uncachedRowCount, col, uncachedNullCount, uncachedMin, uncachedMax);
        SourceMetadata uncached = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return List.of();
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/uncached.parquet";
            }

            @Override
            public Optional<SourceStatistics> statistics() {
                return Optional.of(uncachedStats);
            }
        };

        Map<String, Object> cachedFlatStats = SourceStatisticsSerializer.embedStatistics(
            Map.of(),
            statsWithColumn(cachedRowCount, col, cachedNullCount, cachedMin, cachedMax)
        );
        SourceMetadata cached = new SourceMetadata() {
            @Override
            public List<Attribute> schema() {
                return List.of();
            }

            @Override
            public String sourceType() {
                return "parquet";
            }

            @Override
            public String location() {
                return "s3://bucket/cached.parquet";
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return cachedFlatStats;
            }
        };

        Map<String, Object> merged = ExternalSourceResolver.aggregateFileStatistics(List.of(uncached, cached));
        assertNotNull(merged);
        assertEquals(uncachedRowCount + cachedRowCount, ((Number) merged.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue());
        assertEquals(
            uncachedNullCount + cachedNullCount,
            ((Number) merged.get(SourceStatisticsSerializer.columnNullCountKey(col))).longValue()
        );
        assertEquals(uncachedMin, ((Number) merged.get(SourceStatisticsSerializer.columnMinKey(col))).longValue());
        assertEquals(cachedMax, ((Number) merged.get(SourceStatisticsSerializer.columnMaxKey(col))).longValue());
    }

    private static SourceStatistics statsWithColumn(long rowCount, String columnName, long nullCount, long min, long max) {
        SourceStatistics.ColumnStatistics colStats = new SourceStatistics.ColumnStatistics() {
            @Override
            public OptionalLong nullCount() {
                return OptionalLong.of(nullCount);
            }

            @Override
            public OptionalLong distinctCount() {
                return OptionalLong.empty();
            }

            @Override
            public Optional<Object> minValue() {
                return Optional.of(min);
            }

            @Override
            public Optional<Object> maxValue() {
                return Optional.of(max);
            }
        };
        return new SourceStatistics() {
            @Override
            public OptionalLong rowCount() {
                return OptionalLong.of(rowCount);
            }

            @Override
            public OptionalLong sizeInBytes() {
                return OptionalLong.empty();
            }

            @Override
            public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                return Optional.of(Map.of(columnName, colStats));
            }
        };
    }

    public void testSingleFileSchemaCacheHitAfterMiss() throws Exception {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER), attr("name", DataType.KEYWORD));
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/single.parquet", schema);

        CountingStorageProvider countingProvider = new CountingStorageProvider(Map.of(), schemasByPath);

        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();

        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(settings)) {
            ExternalSourceResolver resolver = createResolverWithCache(countingProvider, schemasByPath, cacheService);

            PlainActionFuture<ExternalSourceResolution> f1 = new PlainActionFuture<>();
            resolver.resolve(List.of("s3://bucket/data/single.parquet"), Map.of(), f1);
            ExternalSourceResolution res1 = f1.actionGet();
            assertNotNull(res1.resolvedSource("s3://bucket/data/single.parquet"));
            assertEquals(1, res1.resolvedSource("s3://bucket/data/single.parquet").fileList().fileCount());

            Map<String, Object> stats1 = cacheService.usageStats();
            assertEquals(1L, stats1.get("schema_cache.misses"));
            assertEquals(0L, stats1.get("schema_cache.hits"));
            assertEquals(1, stats1.get("schema_cache.count"));

            PlainActionFuture<ExternalSourceResolution> f2 = new PlainActionFuture<>();
            resolver.resolve(List.of("s3://bucket/data/single.parquet"), Map.of(), f2);
            ExternalSourceResolution res2 = f2.actionGet();
            assertNotNull(res2.resolvedSource("s3://bucket/data/single.parquet"));
            assertEquals(1, res2.resolvedSource("s3://bucket/data/single.parquet").fileList().fileCount());

            Map<String, Object> stats2 = cacheService.usageStats();
            assertEquals(1L, stats2.get("schema_cache.misses"));
            assertEquals(1L, stats2.get("schema_cache.hits"));
            assertEquals(1, stats2.get("schema_cache.count"));
        }
    }

    public void testSingleFileCacheDisabledBypassesCache() throws Exception {
        List<Attribute> schema = List.of(attr("val", DataType.LONG));
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/d/file.parquet", schema);

        CountingStorageProvider countingProvider = new CountingStorageProvider(Map.of(), schemasByPath);

        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", false)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();

        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(settings)) {
            ExternalSourceResolver resolver = createResolverWithCache(countingProvider, schemasByPath, cacheService);

            for (int i = 0; i < 3; i++) {
                PlainActionFuture<ExternalSourceResolution> f = new PlainActionFuture<>();
                resolver.resolve(List.of("s3://bucket/d/file.parquet"), Map.of(), f);
                ExternalSourceResolution res = f.actionGet();
                assertNotNull(res.resolvedSource("s3://bucket/d/file.parquet"));
            }

            Map<String, Object> stats = cacheService.usageStats();
            assertEquals("schema cache should have no entries when disabled", 0, stats.get("schema_cache.count"));
            assertEquals("schema cache should have no hits when disabled", 0L, stats.get("schema_cache.hits"));
            assertEquals("schema cache should have no misses when disabled", 0L, stats.get("schema_cache.misses"));
        }
    }

    public void testCacheDisabledCallsLoaderEveryTime() throws Exception {
        List<Attribute> schema = List.of(attr("val", DataType.LONG));
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/d/x.parquet", schema);

        List<StorageEntry> listing = List.of(entry("s3://bucket/d/x.parquet", 50));

        CountingStorageProvider countingProvider = new CountingStorageProvider(Map.of("s3://bucket/d/", listing), schemasByPath);

        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", false)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();

        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(settings)) {
            ExternalSourceResolver resolver = createResolverWithCache(countingProvider, schemasByPath, cacheService);

            for (int i = 0; i < 3; i++) {
                PlainActionFuture<ExternalSourceResolution> f = new PlainActionFuture<>();
                resolver.resolve(List.of("s3://bucket/d/*.parquet"), Map.of(), f);
                f.actionGet();
            }

            assertEquals(
                "listing loader should be called on every resolve when cache is disabled",
                3,
                countingProvider.listCallCount.get()
            );
        }
    }

    /**
     * Regression test for #147371: single-file caching path must not NPE when
     * StorageObject.lastModified() returns null (e.g. gRPC/Flight, GCS/Azure fixtures).
     */
    public void testSingleFileCacheWithNullLastModifiedDoesNotThrow() throws Exception {
        List<Attribute> schema = List.of(attr("id", DataType.INTEGER));
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/null-mtime.parquet", schema);

        NullMtimeStorageProvider nullMtimeProvider = new NullMtimeStorageProvider(schemasByPath);

        Settings settings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();

        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(settings)) {
            ExternalSourceResolver resolver = createResolverWithCache(nullMtimeProvider, schemasByPath, cacheService);

            PlainActionFuture<ExternalSourceResolution> f1 = new PlainActionFuture<>();
            resolver.resolve(List.of("s3://bucket/data/null-mtime.parquet"), Map.of(), f1);
            ExternalSourceResolution res1 = f1.actionGet();
            assertNotNull(res1.resolvedSource("s3://bucket/data/null-mtime.parquet"));
            assertEquals(1, res1.resolvedSource("s3://bucket/data/null-mtime.parquet").fileList().fileCount());

            // Second resolve should hit the cache without NPE
            PlainActionFuture<ExternalSourceResolution> f2 = new PlainActionFuture<>();
            resolver.resolve(List.of("s3://bucket/data/null-mtime.parquet"), Map.of(), f2);
            ExternalSourceResolution res2 = f2.actionGet();
            assertNotNull(res2.resolvedSource("s3://bucket/data/null-mtime.parquet"));

            Map<String, Object> stats = cacheService.usageStats();
            assertEquals(1L, stats.get("schema_cache.misses"));
            assertEquals(1L, stats.get("schema_cache.hits"));
        }
    }

    // ===== Helpers =====

    private static Attribute attr(String name, DataType type) {
        return new ReferenceAttribute(Source.EMPTY, null, name, type);
    }

    private static int[] identityIndex(int size) {
        int[] idx = new int[size];
        for (int i = 0; i < size; i++) {
            idx[i] = i;
        }
        return idx;
    }

    private static StorageEntry entry(String path, long length) {
        return new StorageEntry(StoragePath.of(path), length, Instant.EPOCH);
    }

    private ExternalSourceResolution resolveMultiFile(
        String globPattern,
        Map<String, List<Attribute>> schemasByPath,
        List<StorageEntry> listing
    ) throws Exception {
        return resolveMultiFileWithConfig(globPattern, schemasByPath, listing, Map.of());
    }

    private ExternalSourceResolution resolveMultiFileWithConfig(
        String globPattern,
        Map<String, List<Attribute>> schemasByPath,
        List<StorageEntry> listing,
        Map<String, Object> config
    ) throws Exception {
        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        StoragePath sp = StoragePath.of(globPattern);
        listingsByPrefix.put(sp.patternPrefix().toString(), listing);

        ExternalSourceResolver resolver = createResolver(schemasByPath, listingsByPrefix);
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();

        // The resolver treats a missing path key and a present-but-empty config map identically
        // (see ExternalSourceResolver.resolve: pathConfigs.getOrDefault(path, Map.of())), so
        // always forward the per-path config — no special-casing the empty case.
        resolver.resolve(List.of(globPattern), Map.of(globPattern, new HashMap<>(config)), future);
        return future.actionGet();
    }

    /**
     * Resolves a multi-file glob pattern using a StubFormatReader that returns per-file row counts
     * as statistics. This enables testing the aggregated-stats path in resolveMultiFileSource.
     */
    private ExternalSourceResolution resolveMultiFileWithStats(
        String globPattern,
        Map<String, List<Attribute>> schemasByPath,
        Map<String, Long> rowCountsByPath,
        List<StorageEntry> listing
    ) throws Exception {
        return resolveMultiFileWithStats(globPattern, schemasByPath, rowCountsByPath, listing, Map.of());
    }

    private ExternalSourceResolution resolveMultiFileWithStats(
        String globPattern,
        Map<String, List<Attribute>> schemasByPath,
        Map<String, Long> rowCountsByPath,
        List<StorageEntry> listing,
        Map<String, Object> config
    ) throws Exception {
        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        StoragePath sp = StoragePath.of(globPattern);
        listingsByPrefix.put(sp.patternPrefix().toString(), listing);

        StubFormatReaderWithStats formatReader = new StubFormatReaderWithStats(schemasByPath, rowCountsByPath);
        StubStorageProvider storageProvider = new StubStorageProvider(listingsByPrefix, schemasByPath);

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedSchemes() {
                return Set.of("s3");
            }

            @Override
            public Set<FormatSpec> formatSpecs() {
                return Set.of(FormatSpec.of("parquet", ".parquet"));
            }

            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", StorageProviderFactory.noConfigKeys(() -> storageProvider));
            }

            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("parquet", (s, bf) -> formatReader);
            }
        };

        List<DataSourcePlugin> plugins = List.of(plugin);
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);
        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(),
            () -> false
        );

        ExternalSourceResolver resolver = new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module);
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        // The resolver treats a missing path key and a present-but-empty config map identically
        // (see ExternalSourceResolver.resolve: pathConfigs.getOrDefault(path, Map.of())), so
        // always forward the per-path config — no special-casing the empty case.
        resolver.resolve(List.of(globPattern), Map.of(globPattern, new HashMap<>(config)), future);
        return future.actionGet();
    }

    /**
     * The two multi-file schema resolution strategies whose code paths (FFW fast path vs.
     * read-all-and-reconcile path) are covered in this suite. STRICT shares the
     * read-all-and-reconcile path with UNION_BY_NAME, so it is not parameterized here.
     */
    private static final List<FormatReader.SchemaResolution> MULTI_FILE_STRATEGIES = List.of(
        FormatReader.SchemaResolution.FIRST_FILE_WINS,
        FormatReader.SchemaResolution.UNION_BY_NAME
    );

    private static Map<String, Object> configFor(FormatReader.SchemaResolution strategy) {
        return Map.of("schema_resolution", strategy.name().toLowerCase(Locale.ROOT));
    }

    private ExternalSourceResolution resolveSingleFile(String path, Map<String, List<Attribute>> schemasByPath) throws Exception {
        ExternalSourceResolver resolver = createResolver(schemasByPath, Map.of());
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(List.of(path), Map.of(), future);
        return future.actionGet();
    }

    private ExternalSourceResolution resolveMultiplePaths(
        List<String> paths,
        Map<String, List<Attribute>> schemasByPath,
        Map<String, List<StorageEntry>> listingsByPrefix
    ) throws Exception {
        ExternalSourceResolver resolver = createResolver(schemasByPath, listingsByPrefix);
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(paths, Map.of(), future);
        return future.actionGet();
    }

    private ExternalSourceResolver createResolver(
        Map<String, List<Attribute>> schemasByPath,
        Map<String, List<StorageEntry>> listingsByPrefix
    ) {
        StubFormatReader formatReader = new StubFormatReader(schemasByPath);
        StubStorageProvider storageProvider = new StubStorageProvider(listingsByPrefix, schemasByPath);

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedSchemes() {
                return Set.of("s3");
            }

            @Override
            public Set<FormatSpec> formatSpecs() {
                return Set.of(FormatSpec.of("parquet", ".parquet"));
            }

            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", stubStorageProviderFactory(storageProvider));
            }

            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("parquet", (s, bf) -> formatReader);
            }
        };

        List<DataSourcePlugin> plugins = List.of(plugin);
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);
        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(),
            () -> false
        );

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module);
    }

    /**
     * Builds a {@link StorageProviderFactory} that claims every configuration key as consumed.
     * Used by tests that don't care about validation but do thread per-query config through;
     * without this, FileSourceFactory's coordinator validation would reject keys like
     * {@code access_key} that the stub doesn't actually parse.
     */
    private static StorageProviderFactory stubStorageProviderFactory(StorageProvider provider) {
        return new StorageProviderFactory() {
            @Override
            public StorageProvider create(Settings settings) {
                return provider;
            }

            @Override
            public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
                if (config == null || config.isEmpty()) {
                    return Configured.empty(provider);
                }
                return new Configured<>(provider, Set.copyOf(config.keySet()));
            }
        };
    }

    private ExternalSourceResolver createResolverWithCache(
        StorageProvider storageProvider,
        Map<String, List<Attribute>> schemasByPath,
        ExternalSourceCacheService cacheService
    ) {
        StubFormatReader formatReader = new StubFormatReader(schemasByPath);

        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<String> supportedSchemes() {
                return Set.of("s3");
            }

            @Override
            public Set<FormatSpec> formatSpecs() {
                return Set.of(FormatSpec.of("parquet", ".parquet"));
            }

            @Override
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", stubStorageProviderFactory(storageProvider));
            }

            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("parquet", (s, bf) -> formatReader);
            }
        };

        List<DataSourcePlugin> plugins = List.of(plugin);
        DataSourceCapabilities capabilities = DataSourceCapabilities.build(plugins);
        DataSourceModule module = new DataSourceModule(
            plugins,
            capabilities,
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(),
            () -> false
        );

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module, Settings.EMPTY, cacheService);
    }

    // ===== Stub implementations =====

    private static class StubFormatReader implements NoConfigFormatReader {

        private final Map<String, List<Attribute>> schemasByPath;

        StubFormatReader(Map<String, List<Attribute>> schemasByPath) {
            this.schemasByPath = schemasByPath;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            String path = object.path().toString();
            List<Attribute> schema = schemasByPath.get(path);
            if (schema == null) {
                throw new IllegalArgumentException("No schema configured for path: " + path);
            }
            return new StubSourceMetadata(path, schema);
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class StubSourceMetadata implements SourceMetadata {
        private final String location;
        private final List<Attribute> schema;

        StubSourceMetadata(String location, List<Attribute> schema) {
            this.location = location;
            this.schema = schema;
        }

        @Override
        public List<Attribute> schema() {
            return schema;
        }

        @Override
        public String sourceType() {
            return "parquet";
        }

        @Override
        public String location() {
            return location;
        }
    }

    /**
     * A StubFormatReader that also returns per-file row counts as statistics.
     * Used to test the aggregated stats path in multi-file resolution.
     */
    private static class StubFormatReaderWithStats implements NoConfigFormatReader {

        private final Map<String, List<Attribute>> schemasByPath;
        private final Map<String, Long> rowCountsByPath;

        StubFormatReaderWithStats(Map<String, List<Attribute>> schemasByPath, Map<String, Long> rowCountsByPath) {
            this.schemasByPath = schemasByPath;
            this.rowCountsByPath = rowCountsByPath;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            String path = object.path().toString();
            List<Attribute> schema = schemasByPath.get(path);
            if (schema == null) {
                throw new IllegalArgumentException("No schema configured for path: " + path);
            }
            Long rowCount = rowCountsByPath.get(path);
            return new SourceMetadata() {
                @Override
                public List<Attribute> schema() {
                    return schema;
                }

                @Override
                public String sourceType() {
                    return "parquet";
                }

                @Override
                public String location() {
                    return path;
                }

                @Override
                public Optional<SourceStatistics> statistics() {
                    if (rowCount == null) {
                        return Optional.empty();
                    }
                    return Optional.of(new SourceStatistics() {
                        @Override
                        public OptionalLong rowCount() {
                            return OptionalLong.of(rowCount);
                        }

                        @Override
                        public OptionalLong sizeInBytes() {
                            return OptionalLong.empty();
                        }

                        @Override
                        public Optional<Map<String, ColumnStatistics>> columnStatistics() {
                            return Optional.empty();
                        }
                    });
                }
            };
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String formatName() {
            return "parquet";
        }

        @Override
        public List<String> fileExtensions() {
            return List.of(".parquet");
        }

        @Override
        public void close() {}
    }

    private static class StubStorageProvider implements StorageProvider {
        private final Map<String, List<StorageEntry>> listingsByPrefix;
        private final Map<String, List<Attribute>> schemasByPath;

        StubStorageProvider(Map<String, List<StorageEntry>> listingsByPrefix, Map<String, List<Attribute>> schemasByPath) {
            this.listingsByPrefix = listingsByPrefix;
            this.schemasByPath = schemasByPath;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new StubStorageObject(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new StubStorageObject(path, length);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            String prefixStr = prefix.toString();
            List<StorageEntry> entries = listingsByPrefix.getOrDefault(prefixStr, List.of());
            return new StorageIterator() {
                private final Iterator<StorageEntry> it = entries.iterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public StorageEntry next() {
                    if (it.hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return it.next();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public boolean exists(StoragePath path) {
            return schemasByPath.containsKey(path.toString());
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }

    private static class StubStorageObject implements StorageObject {
        private final StoragePath path;
        private final long length;

        StubStorageObject(StoragePath path) {
            this(path, 0);
        }

        StubStorageObject(StoragePath path, long length) {
            this.path = path;
            this.length = length;
        }

        @Override
        public InputStream newStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream newStream(long position, long length) {
            return InputStream.nullInputStream();
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }

    /**
     * Wraps StubStorageProvider with counters for listObjects and metadata (newObject) calls
     * to verify that the cache eliminates redundant loader invocations.
     */
    private static class CountingStorageProvider implements StorageProvider {
        final AtomicInteger listCallCount = new AtomicInteger();
        final AtomicInteger schemaCallCount = new AtomicInteger();
        private final StubStorageProvider delegate;

        CountingStorageProvider(Map<String, List<StorageEntry>> listingsByPrefix, Map<String, List<Attribute>> schemasByPath) {
            this.delegate = new StubStorageProvider(listingsByPrefix, schemasByPath);
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            schemaCallCount.incrementAndGet();
            return delegate.newObject(path);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return delegate.newObject(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return delegate.newObject(path, length, lastModified);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            listCallCount.incrementAndGet();
            return delegate.listObjects(prefix, recursive);
        }

        @Override
        public boolean exists(StoragePath path) {
            return delegate.exists(path);
        }

        @Override
        public List<String> supportedSchemes() {
            return delegate.supportedSchemes();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * StorageProvider whose objects return null for lastModified(), reproducing the
     * conditions that caused #147371 (GCS/Azure fixtures, gRPC/Flight).
     */
    private static class NullMtimeStorageProvider implements StorageProvider {
        private final StubStorageProvider delegate;

        NullMtimeStorageProvider(Map<String, List<Attribute>> schemasByPath) {
            this.delegate = new StubStorageProvider(Map.of(), schemasByPath);
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path) {
                @Override
                public Instant lastModified() {
                    return null;
                }
            };
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return delegate.newObject(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return delegate.newObject(path, length, lastModified);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            return delegate.listObjects(prefix, recursive);
        }

        @Override
        public boolean exists(StoragePath path) {
            return delegate.exists(path);
        }

        @Override
        public List<String> supportedSchemes() {
            return delegate.supportedSchemes();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
