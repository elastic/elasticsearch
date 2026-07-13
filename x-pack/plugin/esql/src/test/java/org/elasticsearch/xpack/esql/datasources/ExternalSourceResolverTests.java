/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;
import org.elasticsearch.xpack.esql.datasources.spi.NoConfigFormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.PassThroughRowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.RowPositionStrategy;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.Mockito.mock;

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

    // _file.* columns are no longer auto-attached to the resolved schema (they are request-driven),
    // so the resolved-schema width assertions below count data columns + partition columns only.

    private static final EncryptionService ENCRYPTION_SERVICE = mock(EncryptionService.class);

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    /**
     * Guards {@link ExternalSourceResolver#FILE_TYPED_FORMATS} — the hand-maintained classification of columnar
     * (self-typed) formats that gates all three columnar declaration rejects (format-on-columnar, strict type mismatch,
     * non-strict retype). The set has no SPI-derived source of truth yet (see the constant's TODO), so pin its exact
     * membership: dropping an entry silently disables the rejects for that format, and a new columnar reader must be
     * added here. A change to this set is a deliberate, reviewed test diff — not a silent drift.
     */
    public void testFileTypedFormatsGatesColumnarRejects() {
        assertEquals(
            Set.of(FormatNameResolver.FORMAT_PARQUET, "orc", FormatNameResolver.FORMAT_PARQUET_RS),
            ExternalSourceResolver.FILE_TYPED_FORMATS
        );
        // Text formats parse into the declared type, so a declared format/retype IS honored — they must NOT be here.
        assertFalse(ExternalSourceResolver.FILE_TYPED_FORMATS.contains("csv"));
        assertFalse(ExternalSourceResolver.FILE_TYPED_FORMATS.contains("tsv"));
        assertFalse(ExternalSourceResolver.FILE_TYPED_FORMATS.contains("ndjson"));
    }

    /**
     * Pins {@link ExternalSourceResolver#COERCING_FILE_TYPED_FORMATS} — the columnar formats whose readers coerce a
     * declared type from the file's physical type (vs strict equality). It must be a subset of the file-typed set, and
     * {@code parquet-rs} must stay OUT of it (it is file-typed but does not implement coercion yet), so a declared
     * retype on parquet-rs still requires strict equality rather than silently coercing.
     */
    public void testCoercingFileTypedFormatsPinned() {
        assertEquals(Set.of(FormatNameResolver.FORMAT_PARQUET, "orc"), ExternalSourceResolver.COERCING_FILE_TYPED_FORMATS);
        assertTrue(ExternalSourceResolver.FILE_TYPED_FORMATS.containsAll(ExternalSourceResolver.COERCING_FILE_TYPED_FORMATS));
        assertTrue(ExternalSourceResolver.FILE_TYPED_FORMATS.contains(FormatNameResolver.FORMAT_PARQUET_RS));
        assertFalse(ExternalSourceResolver.COERCING_FILE_TYPED_FORMATS.contains(FormatNameResolver.FORMAT_PARQUET_RS));
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
            assertEquals("[" + strategy + "] resolved schema width", expectedDataNames.size(), resolvedSchema.size());
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
            assertEquals("[" + strategy + "] resolved schema width", expectedDataNames.size(), resolvedSchema.size());
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
            assertEquals("[" + strategy + "] resolved schema width", 2, resolvedSchema.size());
            assertEquals("[" + strategy + "] resolved column 0 name", "id", resolvedSchema.get(0).name());
            assertEquals("[" + strategy + "] resolved column 1 name", "value", resolvedSchema.get(1).name());
            assertEquals("[" + strategy + "] resolved column 0 type", DataType.LONG, resolvedSchema.get(0).dataType());
            assertEquals("[" + strategy + "] resolved column 1 type", DataType.DOUBLE, resolvedSchema.get(1).dataType());
        }
    }

    /**
     * FIRST_FILE_WINS folds every file's stats under the anchor's schema without enforcing that the other files
     * actually share it. A column whose physical type diverges across files (here {@code ts}: DATETIME/millis in
     * the anchor, DATE_NANOS/nanos in file 2) is read from the divergent file under the anchor schema — its data
     * is misread — so a warm extremum cannot match a scan. The fold must POISON such a column's extrema
     * (safe-miss), while a uniformly-typed column ({@code id}) folds normally.
     */
    public void testFfwAggregatePoisonsExtremaOfDivergentlyTypedColumn() {
        Map<String, Object> f1 = new HashMap<>();
        f1.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 2L);
        f1.put(SourceStatisticsSerializer.columnMinKey("ts"), 1000L);
        f1.put(SourceStatisticsSerializer.columnMaxKey("ts"), 5000L);
        f1.put(SourceStatisticsSerializer.columnMinKey("id"), 1L);
        f1.put(SourceStatisticsSerializer.columnMaxKey("id"), 9L);
        Map<String, Object> f2 = new HashMap<>();
        f2.put(SourceStatisticsSerializer.STATS_ROW_COUNT, 2L);
        f2.put(SourceStatisticsSerializer.columnMinKey("ts"), 2_000_000L);
        f2.put(SourceStatisticsSerializer.columnMaxKey("ts"), 9_000_000L);
        f2.put(SourceStatisticsSerializer.columnMinKey("id"), 3L);
        f2.put(SourceStatisticsSerializer.columnMaxKey("id"), 7L);
        SourceMetadata m1 = new SimpleSourceMetadata(
            List.of(attr("ts", DataType.DATETIME), attr("id", DataType.LONG)),
            "parquet",
            "file:///1.parquet",
            null,
            null,
            f1,
            null
        );
        SourceMetadata m2 = new SimpleSourceMetadata(
            List.of(attr("ts", DataType.DATE_NANOS), attr("id", DataType.LONG)),
            "parquet",
            "file:///2.parquet",
            null,
            null,
            f2,
            null
        );

        Map<String, Object> agg = ExternalSourceResolver.aggregateFileStatistics(List.of(m1, m2), false);
        assertNotNull(agg);
        // ts diverged -> extrema poisoned (value dropped, unservable marker set) -> MIN/MAX(ts) safe-miss to a scan.
        assertNull(agg.get(SourceStatisticsSerializer.columnMinKey("ts")));
        assertNull(agg.get(SourceStatisticsSerializer.columnMaxKey("ts")));
        assertEquals(Boolean.TRUE, agg.get(SourceStatisticsSerializer.columnMinUnservableKey("ts")));
        assertEquals(Boolean.TRUE, agg.get(SourceStatisticsSerializer.columnMaxUnservableKey("ts")));
        // id is uniformly LONG -> folds normally.
        assertEquals(1L, agg.get(SourceStatisticsSerializer.columnMinKey("id")));
        assertEquals(9L, agg.get(SourceStatisticsSerializer.columnMaxKey("id")));
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
                // column order, with -1 for columns the file is missing. _file.* columns are no
                // longer auto-attached, so the metadata schema is exactly the data columns.
                List<String> expectedDataColumns = List.of("col0", "col1", "col2");
                List<Attribute> unifiedSchema = resolved.metadata().schema();
                assertEquals("[" + strategy + "] unified schema width", expectedDataColumns.size(), unifiedSchema.size());
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

    // ===== Deferred eager-stats (requiresStats gating) tests =====

    /**
     * Defer path (non-cacheable): a multi-file FFW resolve with an empty (non-null)
     * {@code pathsRequiringStats} set reads only the anchor footer (1 metadata read), keeps
     * {@code STATS_FILE_COUNT}, and marks stats partial — exactly the state the failed-aggregation
     * fallback produces, so downstream consumers already handle it.
     */
    public void testFirstFileWinsDefersFooterReadsWhenStatsNotRequired() throws Exception {
        AtomicInteger metadataReads = new AtomicInteger();
        ExternalSourceResolution resolution = resolveFfwWithRequirement(threeFileStats(), metadataReads, Set.of(), null);

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource(GLOB);
        assertNotNull(resolved);
        assertEquals("defer must read only the anchor footer", 1, metadataReads.get());
        Map<String, Object> meta = resolved.metadata().sourceMetadata();
        assertEquals("deferred stats must be partial", Boolean.TRUE, meta.get(SourceStatisticsSerializer.STATS_PARTIAL));
        assertEquals("file count is preserved on defer", 3L, meta.get(SourceStatisticsSerializer.STATS_FILE_COUNT));
        // The anchor's own (single-file) stats remain embedded, but STATS_PARTIAL flags them as not
        // representative of the whole glob, so downstream never consumes them as global stats
        // (see testDeferredMetadataNeverConsumedAsGlobalStats). They are NOT the aggregated total.
        assertEquals("anchor-only row count, not the 6000 aggregate", 1000L, meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }

    /**
     * Eager path (non-cacheable): when the path is in {@code pathsRequiringStats}, every file footer
     * is read (anchor schema + N stats reads) and the aggregated global stats are complete
     * (no {@code STATS_PARTIAL}).
     */
    public void testFirstFileWinsEagerlyReadsFootersWhenStatsRequired() throws Exception {
        AtomicInteger metadataReads = new AtomicInteger();
        ExternalSourceResolution resolution = resolveFfwWithRequirement(threeFileStats(), metadataReads, Set.of(GLOB), null);

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource(GLOB);
        assertNotNull(resolved);
        // anchor schema read (1) + per-file stats reads across all 3 files (3) = 4.
        assertEquals("eager must read the anchor footer plus all file footers", 4, metadataReads.get());
        Map<String, Object> meta = resolved.metadata().sourceMetadata();
        assertNull("eager stats are complete, not partial", meta.get(SourceStatisticsSerializer.STATS_PARTIAL));
        assertEquals("aggregated row count across all files", 6000L, meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        assertEquals("file count is stamped on eager too", 3L, meta.get(SourceStatisticsSerializer.STATS_FILE_COUNT));
    }

    /**
     * Legacy {@code null} overload: a {@code null} {@code pathsRequiringStats} keeps the original
     * eager-for-every-path behavior, so all footers are read regardless of query shape.
     */
    public void testFirstFileWinsLegacyNullSetReadsAllFooters() throws Exception {
        AtomicInteger metadataReads = new AtomicInteger();
        ExternalSourceResolution resolution = resolveFfwWithRequirement(threeFileStats(), metadataReads, null, null);

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource(GLOB);
        assertNotNull(resolved);
        assertEquals("legacy null set is eager for all paths", 4, metadataReads.get());
        Map<String, Object> meta = resolved.metadata().sourceMetadata();
        assertNull("legacy eager stats are complete", meta.get(SourceStatisticsSerializer.STATS_PARTIAL));
        assertEquals(6000L, meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
    }

    /**
     * Defer path (cacheable): only the anchor schema is loaded (1 cold load). The per-file stats
     * loop is skipped entirely.
     */
    public void testFirstFileWinsDeferCacheableLoadsAnchorOnly() throws Exception {
        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(cacheEnabledSettings())) {
            CountingStorageProvider provider = new CountingStorageProvider(Map.of(PREFIX, threeFileListing()), threeFileSchemas());
            ExternalSourceResolver resolver = buildStatsResolver(provider, threeFileStats(), null, cacheService);

            ExternalSourceResolution resolution = resolveFfw(resolver, Set.of());
            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource(GLOB);
            assertNotNull(resolved);
            assertEquals("defer loads only the anchor schema", 1, provider.schemaCallCount.get());
            assertEquals(Boolean.TRUE, resolved.metadata().sourceMetadata().get(SourceStatisticsSerializer.STATS_PARTIAL));
        }
    }

    /**
     * Eager path (cacheable, cold): the anchor schema plus every other file is loaded once
     * (N cold loads, anchor reused from cache in the stats loop). Aggregated stats are complete.
     */
    public void testFirstFileWinsEagerCacheableColdLoadsAllFiles() throws Exception {
        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(cacheEnabledSettings())) {
            CountingStorageProvider provider = new CountingStorageProvider(Map.of(PREFIX, threeFileListing()), threeFileSchemas());
            ExternalSourceResolver resolver = buildStatsResolver(provider, threeFileStats(), null, cacheService);

            ExternalSourceResolution resolution = resolveFfw(resolver, Set.of(GLOB));
            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource(GLOB);
            assertNotNull(resolved);
            assertEquals("eager cold-loads all 3 file schemas exactly once", 3, provider.schemaCallCount.get());
            Map<String, Object> meta = resolved.metadata().sourceMetadata();
            assertNull(meta.get(SourceStatisticsSerializer.STATS_PARTIAL));
            assertEquals(6000L, meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        }
    }

    /**
     * Anchor-stats invariant: deferred metadata is {@code STATS_PARTIAL == true} and
     * {@link SplitStats#resolveEffectiveStats} over empty splits returns {@code null} — proving the
     * anchor-only stats are never consumed as global stats downstream.
     */
    public void testDeferredMetadataNeverConsumedAsGlobalStats() throws Exception {
        AtomicInteger metadataReads = new AtomicInteger();
        ExternalSourceResolution resolution = resolveFfwWithRequirement(threeFileStats(), metadataReads, Set.of(), null);

        Map<String, Object> meta = resolution.resolvedSource(GLOB).metadata().sourceMetadata();
        assertEquals(Boolean.TRUE, meta.get(SourceStatisticsSerializer.STATS_PARTIAL));
        assertNull(
            "deferred (partial) anchor stats must not resolve as global split stats",
            SplitStats.resolveEffectiveStats(List.of(), meta)
        );
    }

    /**
     * Regression: the UNION_BY_NAME / STRICT reconciliation path must read every file regardless of
     * {@code pathsRequiringStats} — it needs all schemas to build the unified schema and cannot
     * defer. An empty (defer-everything) set must not change its behavior.
     */
    public void testReconciliationPathReadsAllFilesRegardlessOfStatsRequirement() throws Exception {
        for (FormatReader.SchemaResolution strategy : List.of(
            FormatReader.SchemaResolution.UNION_BY_NAME,
            FormatReader.SchemaResolution.STRICT
        )) {
            AtomicInteger metadataReads = new AtomicInteger();
            // empty pathsRequiringStats would defer under FFW; the reconciliation path ignores it.
            ExternalSourceResolution resolution = resolveFfwWithRequirement(threeFileStats(), metadataReads, Set.of(), strategy);

            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource(GLOB);
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);
            assertEquals("[" + strategy + "] reconciliation must read all files", 3, metadataReads.get());
            Map<String, Object> meta = resolved.metadata().sourceMetadata();
            assertNull("[" + strategy + "] reconciliation stats are complete", meta.get(SourceStatisticsSerializer.STATS_PARTIAL));
            assertEquals("[" + strategy + "] aggregated row count", 6000L, meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        }
    }

    // ----- helpers for the requiresStats tests -----

    private static final String GLOB = "s3://bucket/data/*.parquet";
    private static final String PREFIX = "s3://bucket/data/";

    private static Map<String, List<Attribute>> threeFileSchemas() {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);
        schemasByPath.put("s3://bucket/data/c.parquet", schema);
        return schemasByPath;
    }

    private static Map<String, Long> threeFileRowCounts() {
        Map<String, Long> rowCounts = new HashMap<>();
        rowCounts.put("s3://bucket/data/a.parquet", 1000L);
        rowCounts.put("s3://bucket/data/b.parquet", 2000L);
        rowCounts.put("s3://bucket/data/c.parquet", 3000L);
        return rowCounts;
    }

    private record ThreeFileStats(Map<String, List<Attribute>> schemas, Map<String, Long> rowCounts) {}

    private static ThreeFileStats threeFileStats() {
        return new ThreeFileStats(threeFileSchemas(), threeFileRowCounts());
    }

    private static List<StorageEntry> threeFileListing() {
        return List.of(
            entry("s3://bucket/data/a.parquet", 100),
            entry("s3://bucket/data/b.parquet", 200),
            entry("s3://bucket/data/c.parquet", 300)
        );
    }

    private static Settings cacheEnabledSettings() {
        return Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();
    }

    /**
     * Non-cacheable FFW resolve that counts footer reads (format-reader metadata calls) and threads a
     * {@code pathsRequiringStats} set through the new 5-arg {@code resolve} overload.
     */
    private ExternalSourceResolution resolveFfwWithRequirement(
        ThreeFileStats stats,
        AtomicInteger metadataReadCounter,
        Set<String> pathsRequiringStats,
        FormatReader.SchemaResolution strategy
    ) throws Exception {
        StubStorageProvider storageProvider = new StubStorageProvider(Map.of(PREFIX, threeFileListing()), stats.schemas());
        ExternalSourceResolver resolver = buildStatsResolver(storageProvider, stats, metadataReadCounter, null);
        Map<String, Object> config = configFor(strategy == null ? FormatReader.SchemaResolution.FIRST_FILE_WINS : strategy);
        return resolveFfwWithConfig(resolver, pathsRequiringStats, config);
    }

    private ExternalSourceResolution resolveFfw(ExternalSourceResolver resolver, Set<String> pathsRequiringStats) {
        return resolveFfwWithConfig(resolver, pathsRequiringStats, configFor(FormatReader.SchemaResolution.FIRST_FILE_WINS));
    }

    private ExternalSourceResolution resolveFfwWithConfig(
        ExternalSourceResolver resolver,
        Set<String> pathsRequiringStats,
        Map<String, Object> config
    ) {
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(List.of(GLOB), Map.of(GLOB, new HashMap<>(config)), null, null, pathsRequiringStats, future);
        return future.actionGet();
    }

    /**
     * Builds a resolver around a stats-returning format reader. When {@code metadataReadCounter} is
     * non-null, every footer read (format-reader metadata call) is counted.
     */
    private ExternalSourceResolver buildStatsResolver(
        StorageProvider storageProvider,
        ThreeFileStats stats,
        AtomicInteger metadataReadCounter,
        ExternalSourceCacheService cacheService
    ) {
        StubFormatReaderWithStats formatReader = new StubFormatReaderWithStats(stats.schemas(), stats.rowCounts(), metadataReadCounter);

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
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module, Settings.EMPTY, cacheService);
    }

    // ===== dataset-level aggregate key gating =====

    /**
     * The dataset-level aggregate is ROW-COUNT-ONLY, and under the footer implicit-nulls contract an
     * absent per-column stat reads as "all null" — a footer-format {@code COUNT(col)} served from it
     * would fold {@code rowCount - rowCount = 0}, a wrong answer. The key factory is the single gate
     * that disables put, serve, and promise registration together, so it must refuse implicit-nulls
     * formats and admit text formats (the positive control keeps this test from passing vacuously).
     */
    public void testDatasetAggregateKeyRefusedForImplicitNullsFormats() {
        ExternalSourceResolver resolver = datasetGateResolver(null);

        FileList parquetListing = GlobExpander.fileListOf(
            List.of(entry("s3://bucket/data/a.parquet", 100), entry("s3://bucket/data/b.parquet", 200)),
            "s3://bucket/data/*.parquet"
        );
        assertNull(
            "an implicit-nulls (footer) format must not carry a row-count-only dataset aggregate",
            resolver.datasetAggregateKey(parquetListing, Map.of())
        );

        FileList textListing = GlobExpander.fileListOf(
            List.of(entry("s3://bucket/data/a.ndjson", 100), entry("s3://bucket/data/b.ndjson", 200)),
            "s3://bucket/data/*.ndjson"
        );
        assertNotNull("a text-format listing must qualify (positive control)", resolver.datasetAggregateKey(textListing, Map.of()));
    }

    /**
     * The gate resolves the format the way the READ path does, and any resolution failure refuses
     * rather than throws: the registry throws {@code QlIllegalArgumentException} (not
     * {@code java.lang.IllegalArgumentException}) on an unregistered extension, and the aggregate is
     * an optimization that must never turn a resolvable read into a throw.
     */
    public void testDatasetAggregateKeyUnregisteredExtensionRefusesWithoutThrowing() {
        ExternalSourceResolver resolver = datasetGateResolver(null);
        FileList unknownListing = GlobExpander.fileListOf(
            List.of(entry("s3://bucket/data/a.xyz", 100), entry("s3://bucket/data/b.xyz", 200)),
            "s3://bucket/data/*.xyz"
        );
        assertNull(
            "an unregistered extension must refuse the aggregate, not throw",
            resolver.datasetAggregateKey(unknownListing, Map.of())
        );
    }

    /**
     * The config {@code format} override wins over the file extension, exactly like the read path:
     * {@code format=parquet} over {@code .ndjson}-named files reads the footer contract, so the
     * row-count-only aggregate must be refused even though the extension alone would qualify.
     */
    public void testDatasetAggregateKeyConfigFormatOverridesExtension() {
        ExternalSourceResolver resolver = datasetGateResolver(null);
        FileList ndjsonNamed = GlobExpander.fileListOf(
            List.of(entry("s3://bucket/data/a.ndjson", 100), entry("s3://bucket/data/b.ndjson", 200)),
            "s3://bucket/data/*.ndjson"
        );
        assertNull(
            "format=parquet must gate .ndjson-named files as parquet (config wins over extension)",
            resolver.datasetAggregateKey(ndjsonNamed, Map.of("format", "parquet"))
        );
    }

    /**
     * Duplicate-path guard on the write-through: a comma-separated list can name the same file twice;
     * the reconciliation rail's per-file merge folds a per-path MAP (deduplicated) while the scan reads
     * the listing MULTISET, so memoizing that merge under the file-set fingerprint would persist an undercount
     * beyond eviction. A distinct listing is the positive control.
     */
    public void testDatasetAggregateWriteThroughRefusedForDuplicatePaths() {
        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(Settings.EMPTY)) {
            ExternalSourceResolver resolver = datasetGateResolver(cacheService);
            SourceMetadata referenceMeta = new SimpleSourceMetadata(List.of(), "ndjson", "s3://bucket/data/a.ndjson");
            Map<String, Object> aggregated = Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L);

            String path = "s3://bucket/data/a.ndjson";
            FileList duplicated = GlobExpander.fileListOf(List.of(entry(path, 100), entry(path, 100)), path + "," + path);
            SchemaCacheKey duplicatedKey = resolver.datasetAggregateKey(duplicated, Map.of());
            assertNotNull("the key factory itself does not police duplicates", duplicatedKey);
            Map<String, Object> served = resolver.applyDatasetAggregate(
                new ExternalSourceResolver.DatasetAggregatePrefetch(duplicatedKey, null),
                aggregated,
                duplicated,
                referenceMeta,
                Map.of()
            );
            assertSame("the per-file merge is still served to this query", aggregated, served);
            assertNull("a duplicate-path merge must not be memoized", cacheService.getDatasetAggregate(duplicatedKey));

            FileList distinct = GlobExpander.fileListOf(
                List.of(entry("s3://bucket/data/a.ndjson", 100), entry("s3://bucket/data/b.ndjson", 200)),
                "s3://bucket/data/*.ndjson"
            );
            SchemaCacheKey distinctKey = resolver.datasetAggregateKey(distinct, Map.of());
            resolver.applyDatasetAggregate(
                new ExternalSourceResolver.DatasetAggregatePrefetch(distinctKey, null),
                aggregated,
                distinct,
                referenceMeta,
                Map.of()
            );
            Map<String, Object> memoized = cacheService.getDatasetAggregate(distinctKey);
            assertNotNull("a distinct-path merge writes through (positive control)", memoized);
            assertEquals(100L, memoized.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        }
    }

    /**
     * Hot-path guard: once the aggregate is memoized under the fingerprint key, a repeat warm resolve
     * whose prefetch HIT must NOT re-scan paths and re-write it — the set-identity key guarantees the
     * memoized count is current, and the prefetch's read already kept the entry alive. The differing
     * merge count here is only a probe to observe whether the (skipped) write-through fired.
     */
    public void testDatasetAggregateWriteThroughSkippedWhenPrefetchHit() {
        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(Settings.EMPTY)) {
            ExternalSourceResolver resolver = datasetGateResolver(cacheService);
            SourceMetadata referenceMeta = new SimpleSourceMetadata(List.of(), "ndjson", "s3://bucket/data/a.ndjson");
            FileList distinct = GlobExpander.fileListOf(
                List.of(entry("s3://bucket/data/a.ndjson", 100), entry("s3://bucket/data/b.ndjson", 200)),
                "s3://bucket/data/*.ndjson"
            );
            SchemaCacheKey key = resolver.datasetAggregateKey(distinct, Map.of());

            // First warm resolve, prefetch missed (null): the successful merge writes through.
            resolver.applyDatasetAggregate(
                new ExternalSourceResolver.DatasetAggregatePrefetch(key, null),
                Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L),
                distinct,
                referenceMeta,
                Map.of()
            );
            Map<String, Object> memoized = cacheService.getDatasetAggregate(key);
            assertNotNull("first merge writes through", memoized);
            assertEquals(100L, memoized.get(SourceStatisticsSerializer.STATS_ROW_COUNT));

            // Second warm resolve, prefetch HIT (non-null): the write-through is skipped, so the probe
            // count (999) is NOT persisted — the memoized value stays as first written.
            Map<String, Object> served = resolver.applyDatasetAggregate(
                new ExternalSourceResolver.DatasetAggregatePrefetch(key, memoized),
                Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 999L),
                distinct,
                referenceMeta,
                Map.of()
            );
            assertEquals("the current merge is still served to this query", 999L, served.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
            assertEquals(
                "prefetch hit => write-through skipped, memoized value unchanged",
                100L,
                cacheService.getDatasetAggregate(key).get(SourceStatisticsSerializer.STATS_ROW_COUNT)
            );
        }
    }

    /**
     * The needed-path counters must fire from the serve decision: a needed-and-present serve bumps
     * dataset_aggregate.hits, a needed-and-absent serve bumps dataset_aggregate.misses, and they share
     * the "the per-file merge was incomplete" denominator. Guards against silently zeroing the metric
     * (the get side deliberately counts nothing).
     */
    public void testApplyDatasetAggregateCountsHitAndMissOnNeededPath() {
        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(Settings.EMPTY)) {
            ExternalSourceResolver resolver = datasetGateResolver(cacheService);
            SourceMetadata referenceMeta = new SimpleSourceMetadata(List.of(), "ndjson", "s3://bucket/data/a.ndjson");
            FileList distinct = GlobExpander.fileListOf(
                List.of(entry("s3://bucket/data/a.ndjson", 100), entry("s3://bucket/data/b.ndjson", 200)),
                "s3://bucket/data/*.ndjson"
            );
            SchemaCacheKey key = resolver.datasetAggregateKey(distinct, Map.of());

            // Needed (per-file merge null) AND present (prefetch hit) -> one hit, no miss.
            resolver.applyDatasetAggregate(
                new ExternalSourceResolver.DatasetAggregatePrefetch(key, Map.of(SourceStatisticsSerializer.STATS_ROW_COUNT, 100L)),
                null,
                distinct,
                referenceMeta,
                Map.of()
            );
            assertEquals(1L, cacheService.usageStats().get("dataset_aggregate.hits"));
            assertEquals(0L, cacheService.usageStats().get("dataset_aggregate.misses"));

            // Needed AND absent (prefetch miss) -> one miss, hit unchanged.
            resolver.applyDatasetAggregate(
                new ExternalSourceResolver.DatasetAggregatePrefetch(key, null),
                null,
                distinct,
                referenceMeta,
                Map.of()
            );
            assertEquals(1L, cacheService.usageStats().get("dataset_aggregate.hits"));
            assertEquals(1L, cacheService.usageStats().get("dataset_aggregate.misses"));
        }
    }

    /** Shared parquet+ndjson module for the dataset-aggregate gate tests; see {@link TextAggregatePushdownSupport}. */
    private ExternalSourceResolver datasetGateResolver(ExternalSourceCacheService cacheService) {
        StubFormatReaderWithStats footerReader = new StubFormatReaderWithStats(Map.of(), Map.of());
        // Same stub, but named ndjson and declaring the text contract: an absent column stat safe-misses
        // to a re-scan. formatName() must round-trip through the registry back to THIS reader — the gate
        // resolves reader -> formatName -> findByName, exactly like the read path.
        StubFormatReaderWithStats textReader = new StubFormatReaderWithStats(Map.of(), Map.of()) {
            @Override
            public String formatName() {
                return "ndjson";
            }

            @Override
            public List<String> fileExtensions() {
                return List.of(".ndjson");
            }

            @Override
            public AggregatePushdownSupport aggregatePushdownSupport() {
                return new TextAggregatePushdownSupport();
            }
        };
        DataSourcePlugin plugin = new DataSourcePlugin() {
            @Override
            public Set<FormatSpec> formatSpecs() {
                return Set.of(FormatSpec.of("parquet", ".parquet"), FormatSpec.of("ndjson", ".ndjson"));
            }

            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("parquet", (s, bf) -> footerReader, "ndjson", (s, bf) -> textReader);
            }
        };
        List<DataSourcePlugin> plugins = List.of(plugin);
        DataSourceModule module = new DataSourceModule(
            plugins,
            DataSourceCapabilities.build(plugins),
            Settings.EMPTY,
            blockFactory,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );
        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module, Settings.EMPTY, cacheService);
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

    // ===== Cancellation =====

    /**
     * A multi-file resolve must abort with {@link TaskCancelledException} when the originating query is
     * cancelled mid-flight, and must stop reading further per-file footers rather than scanning the whole
     * glob. The resolver runs on the DIRECT executor here, so footer reads happen sequentially and the
     * cancellation flag (flipped after a couple of reads) deterministically short-circuits the rest.
     */
    public void testMultiFileResolveCancellationStopsReadingFooters() {
        int fileCount = 5;
        int cancelAfter = 2;
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            String path = "s3://bucket/data/f" + i + ".parquet";
            schemasByPath.put(path, schema);
            listing.add(entry(path, 100 + i));
        }

        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        listingsByPrefix.put(StoragePath.of("s3://bucket/data/*.parquet").patternPrefix().toString(), listing);

        AtomicInteger reads = new AtomicInteger(0);
        // Flip cancellation once a couple of footers have been read.
        BooleanSupplier isCancelled = () -> reads.get() >= cancelAfter;

        ExternalSourceResolver resolver = createResolverWithCancellation(schemasByPath, listingsByPrefix, isCancelled, reads);
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(List.of("s3://bucket/data/*.parquet"), Map.of(), future);

        // Cancellation surfaces unwrapped (not wrapped in a generic "Failed to resolve external source").
        expectThrows(TaskCancelledException.class, future::actionGet);
        assertThat(
            "cancellation must stop the resolver before reading every footer; read " + reads.get() + " of " + fileCount,
            reads.get(),
            lessThan(fileCount)
        );
    }

    /**
     * A query already cancelled before resolution starts must perform no footer reads at all: the early
     * cancellation check at the top of {@code resolveSource} aborts before glob expansion, cache listing, or
     * any footer read. Surfaces as {@link TaskCancelledException} with a footer read count of exactly zero.
     */
    public void testResolveCancelledUpFrontReadsNoFooters() {
        int fileCount = 4;
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            String path = "s3://bucket/data/f" + i + ".parquet";
            schemasByPath.put(path, schema);
            listing.add(entry(path, 100 + i));
        }

        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        listingsByPrefix.put(StoragePath.of("s3://bucket/data/*.parquet").patternPrefix().toString(), listing);

        AtomicInteger reads = new AtomicInteger(0);
        ExternalSourceResolver resolver = createResolverWithCancellation(schemasByPath, listingsByPrefix, () -> true, reads);
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(List.of("s3://bucket/data/*.parquet"), Map.of(), future);

        expectThrows(TaskCancelledException.class, future::actionGet);
        assertEquals("a query cancelled before resolution must read zero footers", 0, reads.get());
    }

    /**
     * Cancellation observed while reading a footer on the cacheable FIRST_FILE_WINS stats path must surface as
     * {@link TaskCancelledException}, not be masked as a partial-stats result. The schema cache wraps loader
     * failures in an {@code ExecutionException}, so the resolver cannot rely on the exception type alone — it
     * re-checks cancellation in its partial-stats fallback. Here the format reader flips the cancellation flag
     * and fails the second file's footer read; the resolve must abort with {@code TaskCancelledException} rather
     * than complete with partial (anchor-only) stats.
     */
    public void testCachedMultiFileResolveSurfacesCancellationObservedMidRead() throws Exception {
        int fileCount = 2;
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        Map<String, Long> rowCountsByPath = new HashMap<>();
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            String path = "s3://bucket/data/f" + i + ".parquet";
            schemasByPath.put(path, schema);
            rowCountsByPath.put(path, 10L);
            listing.add(entry(path, 100 + i));
        }

        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        listingsByPrefix.put(StoragePath.of("s3://bucket/data/*.parquet").patternPrefix().toString(), listing);

        AtomicInteger reads = new AtomicInteger(0);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        // The lex-smallest file (f0) is the anchor; f1 is therefore only read inside the aggregate loop. Fail
        // f1's footer read with the query already flipped to cancelled, simulating a read that aborts because the
        // client cancelled mid-flight — exercising the partial-stats fallback's cancellation re-check.
        String failOnPathSuffix = "f1.parquet";

        Settings cacheSettings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();

        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(cacheSettings)) {
            ExternalSourceResolver resolver = createCachedResolverFailingMidRead(
                schemasByPath,
                rowCountsByPath,
                listingsByPrefix,
                cacheService,
                cancelled,
                reads,
                failOnPathSuffix
            );
            PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
            resolver.resolve(
                List.of("s3://bucket/data/*.parquet"),
                Map.of("s3://bucket/data/*.parquet", new HashMap<>(configFor(FormatReader.SchemaResolution.FIRST_FILE_WINS))),
                future
            );

            // Without the cancellation re-check the wrapped failure would degrade to partial stats and the resolve
            // would succeed; instead it must surface cancellation.
            expectThrows(TaskCancelledException.class, future::actionGet);
            assertTrue("the failing footer read must have flipped the query to cancelled", cancelled.get());
        }
    }

    /**
     * Cancellation observed while reading the FIRST_FILE_WINS anchor footer (before the per-file aggregate loop is
     * even reached) must surface as {@link TaskCancelledException}, not as a generic resolution error. The anchor
     * read happens outside the aggregate loop and the cache wraps the failure in an {@code ExecutionException}, so
     * the resolver re-checks cancellation in its failure path. Here the format reader fails the anchor (lex-smallest)
     * file's footer read with the query already flipped to cancelled.
     */
    public void testCachedMultiFileResolveSurfacesCancellationDuringAnchorRead() throws Exception {
        int fileCount = 2;
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        Map<String, Long> rowCountsByPath = new HashMap<>();
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < fileCount; i++) {
            String path = "s3://bucket/data/f" + i + ".parquet";
            schemasByPath.put(path, schema);
            rowCountsByPath.put(path, 10L);
            listing.add(entry(path, 100 + i));
        }

        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        listingsByPrefix.put(StoragePath.of("s3://bucket/data/*.parquet").patternPrefix().toString(), listing);

        AtomicInteger reads = new AtomicInteger(0);
        AtomicBoolean cancelled = new AtomicBoolean(false);
        // f0 is the lex-smallest file, hence the FFW anchor read that runs before the aggregate loop.
        String failOnPathSuffix = "f0.parquet";

        Settings cacheSettings = Settings.builder()
            .put("esql.source.cache.size", "10mb")
            .put("esql.source.cache.enabled", true)
            .put("esql.source.cache.schema.ttl", "5m")
            .put("esql.source.cache.listing.ttl", "30s")
            .build();

        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(cacheSettings)) {
            ExternalSourceResolver resolver = createCachedResolverFailingMidRead(
                schemasByPath,
                rowCountsByPath,
                listingsByPrefix,
                cacheService,
                cancelled,
                reads,
                failOnPathSuffix
            );
            PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
            resolver.resolve(
                List.of("s3://bucket/data/*.parquet"),
                Map.of("s3://bucket/data/*.parquet", new HashMap<>(configFor(FormatReader.SchemaResolution.FIRST_FILE_WINS))),
                future
            );

            expectThrows(TaskCancelledException.class, future::actionGet);
            assertTrue("the failing anchor read must have flipped the query to cancelled", cancelled.get());
        }
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
        assertEquals(5, resolvedSchema.size());
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
        assertEquals(3, resolvedSchema.size());
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
        assertEquals(2, resolvedSchema.size());
        assertEquals("name", resolvedSchema.get(0).name());
        assertEquals("year", resolvedSchema.get(1).name());
        // Partition column type should be INTEGER (from path), not KEYWORD (from data)
        assertEquals(DataType.INTEGER, resolvedSchema.get(1).dataType());

        // Shadowing the physical 'year' column emits a one-time client warning (summary + one detail).
        List<String> warnings = drainWarnings();
        assertEquals(2, warnings.size());
        assertThat(warnings.get(0), containsString("shadowed by same-named Hive partition keys"));
        assertThat(warnings.get(1), containsString("physical column [year] is shadowed"));
    }

    /**
     * Per-file {@code schemaMap} contract under a partition/physical-column collision, across every
     * schema resolution strategy. The data files carry a physical {@code year} column that collides
     * with the {@code year=...} partition key; shadowing must drop the physical column from the
     * unified schema and from each per-file mapping's <em>output</em>, while preserving the file's
     * physical schema so a positional reader (e.g. CSV) still parses every column.
     * <p>
     * Locks the reconciliation-path fix ({@code shadowPartitionCollisions}) for {@code UNION_BY_NAME}
     * and {@code STRICT} alongside the {@code FIRST_FILE_WINS} fast path: the coordinator schema is
     * data-only with the partition column appended, and every per-file mapping is data-only width and
     * non-identity. A regression in the recomputed mapping width or a dropped/added cast would fail
     * here even though {@link #testPartitionColumnConflictPartitionWins} (default {@code UNION_BY_NAME})
     * only checks the coordinator schema and the warning.
     */
    public void testCollisionSchemaMapDropsPhysicalColumnPerStrategy() throws Exception {
        // Identical schemas across files so STRICT can run; 'year' (KEYWORD) collides with the partition key.
        List<Attribute> schema = List.of(attr("year", DataType.KEYWORD), attr("name", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/year=2024/file1.parquet", schema);
        schemasByPath.put("s3://bucket/data/year=2023/file2.parquet", schema);

        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet", 100),
            entry("s3://bucket/data/year=2023/file2.parquet", 200)
        );

        for (FormatReader.SchemaResolution strategy : List.of(
            FormatReader.SchemaResolution.FIRST_FILE_WINS,
            FormatReader.SchemaResolution.UNION_BY_NAME,
            FormatReader.SchemaResolution.STRICT
        )) {
            ExternalSourceResolution resolution = resolveMultiFileWithConfig(
                "s3://bucket/data/year=*/*.parquet",
                schemasByPath,
                listing,
                configFor(strategy)
            );

            ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/year=*/*.parquet");
            assertNotNull("[" + strategy + "] resolved source must not be null", resolved);

            // Coordinator schema: physical 'year' shadowed, partition 'year' (INTEGER from path) appended after data.
            // _file.* columns are request-driven now, so the resolved schema is just [name, year].
            List<Attribute> resolvedSchema = resolved.metadata().schema();
            assertEquals("[" + strategy + "] schema width", 2, resolvedSchema.size());
            assertEquals("[" + strategy + "] data column kept", "name", resolvedSchema.get(0).name());
            assertEquals("[" + strategy + "] partition column appended", "year", resolvedSchema.get(1).name());
            assertEquals("[" + strategy + "] partition type from path", DataType.INTEGER, resolvedSchema.get(1).dataType());

            // Per-file schemaMap: the physical schema is preserved (positional readers parse every column);
            // the mapping is data-only width 1 ('name' only) and non-identity (drops the physical 'year').
            Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap = resolved.schemaMap();
            assertEquals("[" + strategy + "] one schemaMap entry per file", 2, schemaMap.size());
            for (Map.Entry<StoragePath, SchemaReconciliation.FileSchemaInfo> e : schemaMap.entrySet()) {
                assertEquals(
                    "[" + strategy + "] " + e.getKey() + ": file schema keeps the physical 'year' column",
                    schema,
                    e.getValue().fileSchema().attributes()
                );
                ColumnMapping mapping = e.getValue().mapping();
                assertNotNull("[" + strategy + "] " + e.getKey() + ": mapping must be set", mapping);
                assertEquals("[" + strategy + "] " + e.getKey() + ": mapping width is data-only", 1, mapping.width());
                assertFalse("[" + strategy + "] " + e.getKey() + ": mapping is non-identity", mapping.isIdentity());
                // 'name' is at physical position 1; the shadowed physical 'year' (position 0) is not read.
                assertEquals("[" + strategy + "] " + e.getKey() + ": 'name' maps to physical position 1", 1, mapping.localIndex(0));
                assertNull("[" + strategy + "] " + e.getKey() + ": no cast on the kept column", mapping.cast(0));
            }

            // Every strategy emits the one-time shadow warning; drain so teardown stays clean.
            List<String> warnings = drainWarnings();
            assertEquals("[" + strategy + "] summary + one detail", 2, warnings.size());
            assertThat(
                "[" + strategy + "] detail names the shadowed column",
                warnings.get(1),
                containsString("physical column [year] is shadowed")
            );
        }
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
        assertEquals(2, resolvedSchema.size());
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
        assertEquals(3, resolvedSchema.size());
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

    public void testEnrichSchemaWithPartitionColumnsShadowsCollidingPhysicalColumn() {
        // Collision: a physical column 'year' coexists with a same-named Hive partition key. The
        // resolver drops the physical column and appends the partition ReferenceAttribute at the
        // tail (Spark/DuckDB shadowing: path-derived value wins), keeping the schema width stable.
        List<Attribute> originalSchema = List.of(
            attr("id", DataType.INTEGER),
            attr("year", DataType.INTEGER),
            attr("value", DataType.KEYWORD)
        );
        ExternalSourceMetadata metadata = createStubMetadata("s3://bucket/data/*.parquet", originalSchema);

        LinkedHashMap<String, DataType> partCols = new LinkedHashMap<>();
        partCols.put("year", DataType.INTEGER);
        PartitionMetadata partitions = new PartitionMetadata(partCols, Map.of());

        ExternalSourceMetadata enriched = ExternalSourceResolver.enrichSchemaWithPartitionColumns(metadata, partitions);
        List<Attribute> schema = enriched.schema();

        // Physical 'year' dropped, partition 'year' appended after the surviving data columns.
        assertEquals(3, schema.size());
        assertEquals("id", schema.get(0).name());
        assertEquals("value", schema.get(1).name());
        assertEquals("year", schema.get(2).name());
        assertThat("the surviving 'year' is the partition ReferenceAttribute", schema.get(2), instanceOf(ReferenceAttribute.class));

        // A one-time summary plus one detail per shadowed column is recorded on the response headers.
        List<String> warnings = drainWarnings();
        assertEquals(2, warnings.size());
        assertThat(warnings.get(0), containsString("shadowed by same-named Hive partition keys"));
        assertThat(warnings.get(1), containsString("physical column [year] is shadowed"));
    }

    public void testEnrichSchemaWithPartitionColumnsNoCollisionEmitsNoWarning() {
        // No name overlap between data columns and partition keys: no shadow warning is emitted.
        List<Attribute> originalSchema = List.of(attr("id", DataType.INTEGER), attr("value", DataType.KEYWORD));
        ExternalSourceMetadata metadata = createStubMetadata("s3://bucket/data/*.parquet", originalSchema);

        LinkedHashMap<String, DataType> partCols = new LinkedHashMap<>();
        partCols.put("year", DataType.INTEGER);
        PartitionMetadata partitions = new PartitionMetadata(partCols, Map.of());

        ExternalSourceResolver.enrichSchemaWithPartitionColumns(metadata, partitions);

        assertNull("no collision means no Warning header", threadContext.getResponseHeaders().get("Warning"));
    }

    private List<String> drainWarnings() {
        List<String> raw = threadContext.getResponseHeaders().getOrDefault("Warning", List.of());
        List<String> messages = raw.stream().map(s -> HeaderWarning.extractWarningValueFromWarningHeader(s, false)).toList();
        // stashContext installs a fresh empty context, clearing the recorded Warning headers so the
        // ESTestCase.ensureNoWarnings() teardown does not flag them and subsequent resolves start clean.
        threadContext.stashContext();
        return messages;
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

    // ===== Config validation =====

    /**
     * Unknown configuration keys must be rejected by the resolver before any factory consumer
     * (resolveMetadata or operatorFactory) is invoked.
     */
    public void testResolverRejectsUnknownConfigKeyOnSingleFilePath() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));
        Map<String, List<Attribute>> schemasByPath = Map.of("s3://bucket/data/file.parquet", schema);

        ExternalSourceResolver resolver = createStrictValidationResolver(schemasByPath, Map.of(), new AtomicInteger());
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(
            List.of("s3://bucket/data/file.parquet"),
            Map.of("s3://bucket/data/file.parquet", Map.of("bogus_unknown_key", "value")),
            future
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("bogus_unknown_key"));
    }

    /**
     * Unknown config keys are rejected before resolveMetadata is invoked on the cache-miss path.
     * Confirmed by asserting zero schema reads on validation failure: if validation fired only
     * inside resolveMetadata, the format reader would be reached first.
     */
    public void testResolverRejectsUnknownConfigKeyBeforeAnyFactoryRead() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));
        Map<String, List<Attribute>> schemasByPath = Map.of("s3://bucket/data/file.parquet", schema);

        AtomicInteger readerCallCount = new AtomicInteger();
        ExternalSourceResolver resolver = createStrictValidationResolver(schemasByPath, Map.of(), readerCallCount);
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(
            List.of("s3://bucket/data/file.parquet"),
            Map.of("s3://bucket/data/file.parquet", Map.of("bogus_unknown_key", "value")),
            future
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertThat(e.getMessage(), containsString("bogus_unknown_key"));
        assertEquals("validateConfig must fire before resolveMetadata; the format reader must not be reached", 0, readerCallCount.get());
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

        Map<String, Object> merged = ExternalSourceResolver.aggregateFileStatistics(List.of(uncached, cached), true);
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
        assertNull(ExternalSourceResolver.aggregateFileStatistics(List.of(uncached, cached, missing), true));
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

        Map<String, Object> merged = ExternalSourceResolver.aggregateFileStatistics(List.of(uncached, cached), true);
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

    /**
     * Builds a resolver whose storage provider claims no config keys, so any key not in
     * {@link FileSourceFactory#COORDINATOR_KEYS} or the format reader's recognised set is
     * rejected by {@code validateConfig}. {@code readerCallCount} is incremented on every
     * {@link FormatReader#metadata} call.
     */
    private ExternalSourceResolver createStrictValidationResolver(
        Map<String, List<Attribute>> schemasByPath,
        Map<String, List<StorageEntry>> listingsByPrefix,
        AtomicInteger readerCallCount
    ) {
        StubFormatReader formatReader = new StubFormatReader(schemasByPath) {
            @Override
            public SourceMetadata metadata(StorageObject object) {
                readerCallCount.incrementAndGet();
                return super.metadata(object);
            }
        };
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
                // noConfigKeys: the storage provider claims no config keys, so any unknown key
                // is not consumed here and must be caught by ConfigKeyValidator.
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
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );
        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module, Settings.EMPTY, null);
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
            new DataSourceCredentials(ENCRYPTION_SERVICE),
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
            new DataSourceCredentials(ENCRYPTION_SERVICE),
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

    /**
     * Builds a resolver wired with an {@code isCancelled} supplier and a format reader that counts footer
     * reads, so cancellation behavior can be observed end-to-end. Runs on the DIRECT executor so reads are
     * sequential and deterministic.
     */
    private ExternalSourceResolver createResolverWithCancellation(
        Map<String, List<Attribute>> schemasByPath,
        Map<String, List<StorageEntry>> listingsByPrefix,
        BooleanSupplier isCancelled,
        AtomicInteger readCounter
    ) {
        NoConfigFormatReader formatReader = new NoConfigFormatReader() {
            @Override
            public RowPositionStrategy rowPositionStrategy() {
                return PassThroughRowPositionStrategy.INSTANCE;
            }

            @Override
            public SourceMetadata metadata(StorageObject object) {
                readCounter.incrementAndGet();
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
        };
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
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module, Settings.EMPTY, null, isCancelled);
    }

    /**
     * Builds a cacheable resolver whose format reader returns per-file row-count statistics, counts footer reads,
     * and on the {@code failOnRead}-th read flips {@code cancelled} to {@code true} before throwing — modelling a
     * footer read that aborts because the originating query was cancelled mid-flight. The resolver's cancellation
     * supplier is wired to {@code cancelled}, so the partial-stats fallback can re-observe the cancellation.
     */
    private ExternalSourceResolver createCachedResolverFailingMidRead(
        Map<String, List<Attribute>> schemasByPath,
        Map<String, Long> rowCountsByPath,
        Map<String, List<StorageEntry>> listingsByPrefix,
        ExternalSourceCacheService cacheService,
        AtomicBoolean cancelled,
        AtomicInteger readCounter,
        String failOnPathSuffix
    ) {
        NoConfigFormatReader formatReader = new NoConfigFormatReader() {
            @Override
            public RowPositionStrategy rowPositionStrategy() {
                return PassThroughRowPositionStrategy.INSTANCE;
            }

            @Override
            public SourceMetadata metadata(StorageObject object) {
                readCounter.incrementAndGet();
                if (object.path().toString().endsWith(failOnPathSuffix)) {
                    cancelled.set(true);
                    throw new IllegalStateException("footer read failed after cancellation");
                }
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
        };
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
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module, Settings.EMPTY, cacheService, cancelled::get);
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
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module, Settings.EMPTY, cacheService);
    }

    // ===== Async fan-out tests =====

    /**
     * The multi-file fan-out must overlap per-file metadata reads up to the configured permit count
     * even when the resolver executor is a single thread, and must never exceed it. A synchronous /
     * thread-per-read resolver pinned to one thread could only ever have one read in flight; observing
     * a max in-flight equal to the permit count therefore proves both the permit bound and that the
     * pool thread is released across the (simulated) network read.
     */
    public void testAsyncFanOutRespectsPermitBoundBeyondResolverThreads() throws Exception {
        int permits = 4;
        int fileCount = 40;

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        List<StorageEntry> listing = new java.util.ArrayList<>();
        List<Attribute> schema = List.of(attr("emp_no", DataType.INTEGER), attr("name", DataType.KEYWORD));
        for (int i = 0; i < fileCount; i++) {
            String path = String.format(Locale.ROOT, "s3://bucket/data/file%02d.parquet", i);
            schemasByPath.put(path, schema);
            listing.add(entry(path, 100 + i));
        }

        ExecutorService resolverExecutor = Executors.newSingleThreadExecutor();
        ExecutorService readPool = Executors.newFixedThreadPool(permits);
        CountDownLatch gate = new CountDownLatch(1);
        AsyncStubFormatReader reader = new AsyncStubFormatReader(schemasByPath, readPool, gate, permits, null);
        try {
            String glob = "s3://bucket/data/*.parquet";
            ExternalSourceResolution resolution = resolveWithAsyncReader(glob, schemasByPath, listing, reader, resolverExecutor, permits);

            assertNotNull(resolution.resolvedSource(glob));
            assertEquals("max in-flight reads must equal the permit count", permits, reader.maxInFlight.get());
            assertEquals("all files must be read", fileCount, reader.totalReads.get());
        } finally {
            resolverExecutor.shutdownNow();
            readPool.shutdownNow();
        }
    }

    /**
     * A single per-file read failure on the reconciliation path (UNION_BY_NAME) must fail the whole
     * resolve promptly rather than hang or return a partial result. The failure originates in the
     * async reader and must surface through the listener-driven gather.
     */
    public void testAsyncFanOutFailsFastOnReadError() throws Exception {
        int fileCount = 12;
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        List<StorageEntry> listing = new java.util.ArrayList<>();
        List<Attribute> schema = List.of(attr("emp_no", DataType.INTEGER), attr("name", DataType.KEYWORD));
        String failPath = null;
        for (int i = 0; i < fileCount; i++) {
            String path = String.format(Locale.ROOT, "s3://bucket/data/file%02d.parquet", i);
            schemasByPath.put(path, schema);
            listing.add(entry(path, 100 + i));
            if (i == fileCount / 2) {
                failPath = path;
            }
        }

        ExecutorService resolverExecutor = Executors.newSingleThreadExecutor();
        ExecutorService readPool = Executors.newFixedThreadPool(4);
        // gate == null: reads complete without a concurrency rendezvous so the fast-fail short-circuit
        // (which drains the remaining files without issuing reads) cannot deadlock on an unmet barrier.
        AsyncStubFormatReader reader = new AsyncStubFormatReader(schemasByPath, readPool, null, 0, failPath);
        try {
            String glob = "s3://bucket/data/*.parquet";
            Map<String, Object> config = Map.of("schema_resolution", "union_by_name");

            Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
            StoragePath sp = StoragePath.of(glob);
            listingsByPrefix.put(sp.patternPrefix().toString(), listing);
            ExternalSourceResolver resolver = createResolverWithAsyncReader(schemasByPath, listingsByPrefix, reader, resolverExecutor, 4);

            PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
            resolver.resolve(List.of(glob), Map.of(glob, new HashMap<>(config)), future);

            Exception e = expectThrows(Exception.class, () -> future.actionGet(30, TimeUnit.SECONDS));
            assertThat(e.getMessage(), containsString("Failed to resolve metadata"));
        } finally {
            resolverExecutor.shutdownNow();
            readPool.shutdownNow();
        }
    }

    // ===== Security context preservation tests =====

    /**
     * Regression test for a lost security context (issue 152978): when an async metadata read
     * completes on a thread the resolver does not control -- e.g. a native async storage SDK's own
     * I/O thread (AWS's Netty-backed S3 client, for a real Parquet-on-S3 query), which never had the
     * calling request's {@link ThreadContext} installed -- the {@code resolve()} completion listener
     * must still observe the calling request's context. Without the fix, the listener chain runs
     * synchronously on that foreign thread all the way back into {@code EsqlSession} and the
     * subsequent compute transport send, which finds no {@code Authentication} in context and throws
     * "there should always be a user". {@link AsyncStubFormatReader} stands in for the native SDK by
     * completing on a dedicated pool distinct from both the resolver executor and this test thread.
     */
    public void testResolveRestoresCallerThreadContextAcrossAsyncCompletion() throws Exception {
        String headerName = "x-test-auth-marker";
        String headerValue = "authenticated-user";
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader(headerName, headerValue);

        String path = "s3://bucket/data/file.parquet";
        Map<String, List<Attribute>> schemasByPath = Map.of(path, List.of(attr("id", DataType.LONG)));
        String glob = "s3://bucket/data/*.parquet";
        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        listingsByPrefix.put(StoragePath.of(glob).patternPrefix().toString(), List.of(entry(path, 100)));

        ExecutorService resolverExecutor = Executors.newSingleThreadExecutor();
        // Distinct from resolverExecutor and from this test thread: stands in for a native async
        // storage SDK's own I/O thread, which the resolver never dispatches onto and which never had
        // this request's ThreadContext installed.
        ExecutorService ioPool = Executors.newSingleThreadExecutor();
        AsyncStubFormatReader reader = new AsyncStubFormatReader(schemasByPath, ioPool, null, 0, null);
        try {
            ExternalSourceResolver resolver = createResolverWithAsyncReader(
                schemasByPath,
                listingsByPrefix,
                reader,
                resolverExecutor,
                ExternalSourceResolver.DEFAULT_METADATA_READ_CONCURRENCY,
                threadContext
            );

            AtomicReference<String> observedHeaderOnResponse = new AtomicReference<>();
            AtomicReference<Thread> completionThread = new AtomicReference<>();
            PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
            // Captures the header inside the resolve() completion callback itself: ContextPreservingActionListener
            // only restores the calling request's context for the duration of that callback, so the assertion must
            // happen there rather than after future.actionGet() returns on this (unrelated) test thread.
            ActionListener<ExternalSourceResolution> capturingListener = ActionListener.wrap(resolution -> {
                observedHeaderOnResponse.set(threadContext.getHeader(headerName));
                completionThread.set(Thread.currentThread());
                future.onResponse(resolution);
            }, future::onFailure);

            resolver.resolve(List.of(glob), Map.of(glob, Map.of()), capturingListener);
            ExternalSourceResolution resolution = future.actionGet(30, TimeUnit.SECONDS);

            assertNotNull(resolution.resolvedSource(glob));
            assertNotEquals(
                "the completion must run off this test thread to actually exercise cross-thread context restoration",
                Thread.currentThread(),
                completionThread.get()
            );
            assertEquals(
                "the calling request's ThreadContext header must be visible inside the resolve() completion listener "
                    + "even though the async metadata read completed on an unrelated I/O thread",
                headerValue,
                observedHeaderOnResponse.get()
            );
        } finally {
            resolverExecutor.shutdownNow();
            ioPool.shutdownNow();
        }
    }

    private ExternalSourceResolution resolveWithAsyncReader(
        String glob,
        Map<String, List<Attribute>> schemasByPath,
        List<StorageEntry> listing,
        FormatReader reader,
        Executor resolverExecutor,
        int permits
    ) {
        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        StoragePath sp = StoragePath.of(glob);
        listingsByPrefix.put(sp.patternPrefix().toString(), listing);
        ExternalSourceResolver resolver = createResolverWithAsyncReader(schemasByPath, listingsByPrefix, reader, resolverExecutor, permits);
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(List.of(glob), Map.of(glob, new HashMap<>()), future);
        return future.actionGet(30, TimeUnit.SECONDS);
    }

    private ExternalSourceResolver createResolverWithAsyncReader(
        Map<String, List<Attribute>> schemasByPath,
        Map<String, List<StorageEntry>> listingsByPrefix,
        FormatReader formatReader,
        Executor resolverExecutor,
        int permits
    ) {
        return createResolverWithAsyncReader(schemasByPath, listingsByPrefix, formatReader, resolverExecutor, permits, null);
    }

    private ExternalSourceResolver createResolverWithAsyncReader(
        Map<String, List<Attribute>> schemasByPath,
        Map<String, List<StorageEntry>> listingsByPrefix,
        FormatReader formatReader,
        Executor resolverExecutor,
        int permits,
        @Nullable ThreadContext threadContext
    ) {
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
            new DataSourceCredentials(ENCRYPTION_SERVICE),
            () -> false
        );
        return new ExternalSourceResolver(resolverExecutor, module, Settings.EMPTY, null, null, permits, threadContext);
    }

    /**
     * A {@link FormatReader} whose {@link #metadataAsync} completes on a dedicated read pool (never on
     * the resolver executor passed to it), simulating a non-blocking footer read. It records the peak
     * number of concurrently in-flight reads. When a {@code gate} is supplied, each read blocks on it
     * until {@code permits} reads are concurrently in flight (the gate is released by the read that
     * first observes that level), which makes the observed peak deterministic; when {@code gate} is
     * {@code null} reads complete without rendezvous. A configured {@code failPath} fails that file's
     * read with an {@link IOException}.
     */
    private static class AsyncStubFormatReader implements NoConfigFormatReader {
        private final Map<String, List<Attribute>> schemasByPath;
        private final ExecutorService readPool;
        private final CountDownLatch gate;
        private final int permits;
        private final String failPath;
        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger maxInFlight = new AtomicInteger();
        final AtomicInteger totalReads = new AtomicInteger();

        AsyncStubFormatReader(
            Map<String, List<Attribute>> schemasByPath,
            ExecutorService readPool,
            CountDownLatch gate,
            int permits,
            String failPath
        ) {
            this.schemasByPath = schemasByPath;
            this.readPool = readPool;
            this.gate = gate;
            this.permits = permits;
            this.failPath = failPath;
        }

        @Override
        public void metadataAsync(StorageObject object, Executor executor, ActionListener<SourceMetadata> listener) {
            readPool.execute(() -> {
                String path = object.path().toString();
                if (path.equals(failPath)) {
                    listener.onFailure(new IOException("simulated read failure for " + path));
                    return;
                }
                try {
                    int cur = inFlight.incrementAndGet();
                    maxInFlight.accumulateAndGet(cur, Math::max);
                    totalReads.incrementAndGet();
                    if (gate != null) {
                        if (cur >= permits) {
                            gate.countDown();
                        }
                        gate.await(30, TimeUnit.SECONDS);
                    }
                    inFlight.decrementAndGet();
                    listener.onResponse(metadata(object));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
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
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        @Override
        public void close() {}
    }

    // ===== Stub implementations =====

    private static class StubFormatReader implements NoConfigFormatReader {
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

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
        @Override
        public RowPositionStrategy rowPositionStrategy() {
            return PassThroughRowPositionStrategy.INSTANCE;
        }

        private final Map<String, List<Attribute>> schemasByPath;
        private final Map<String, Long> rowCountsByPath;
        private final AtomicInteger metadataReadCounter;

        StubFormatReaderWithStats(Map<String, List<Attribute>> schemasByPath, Map<String, Long> rowCountsByPath) {
            this(schemasByPath, rowCountsByPath, null);
        }

        StubFormatReaderWithStats(
            Map<String, List<Attribute>> schemasByPath,
            Map<String, Long> rowCountsByPath,
            AtomicInteger metadataReadCounter
        ) {
            this.schemasByPath = schemasByPath;
            this.rowCountsByPath = rowCountsByPath;
            this.metadataReadCounter = metadataReadCounter;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            if (metadataReadCounter != null) {
                metadataReadCounter.incrementAndGet();
            }
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
            // Listing-hinted construction is a schema-object creation too (the async multi-file path
            // now builds the object from listing length/mtime instead of a bare newObject + exists()).
            schemaCallCount.incrementAndGet();
            return delegate.newObject(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            schemaCallCount.incrementAndGet();
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
