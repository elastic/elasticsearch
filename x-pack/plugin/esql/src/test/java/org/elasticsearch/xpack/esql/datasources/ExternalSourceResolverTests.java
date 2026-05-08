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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;
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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for ExternalSourceResolver schema resolution behavior.
 * Validates FIRST_FILE_WINS (current behavior) and future STRICT / UNION_BY_NAME strategies
 * using mock FormatReader instances that return controlled schemas per StorageObject.
 */
public class ExternalSourceResolverTests extends ESTestCase {

    private BlockFactory blockFactory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("test")).build();
    }

    // ===== FIRST_FILE_WINS tests (current behavior) =====

    public void testFirstFileWinsUsesFirstSchema() throws Exception {
        List<Attribute> schema1 = List.of(attr("emp_no", DataType.INTEGER), attr("name", DataType.KEYWORD));
        List<Attribute> schema2 = List.of(attr("emp_no", DataType.INTEGER), attr("name", DataType.KEYWORD), attr("extra", DataType.LONG));
        List<Attribute> schema3 = List.of(attr("emp_no", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/file1.parquet", schema1);
        schemasByPath.put("s3://bucket/data/file2.parquet", schema2);
        schemasByPath.put("s3://bucket/data/file3.parquet", schema3);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(
                entry("s3://bucket/data/file1.parquet", 100),
                entry("s3://bucket/data/file2.parquet", 200),
                entry("s3://bucket/data/file3.parquet", 300)
            )
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        List<Attribute> resolvedSchema = resolved.metadata().schema();
        assertEquals(2, resolvedSchema.size());
        assertEquals("emp_no", resolvedSchema.get(0).name());
        assertEquals("name", resolvedSchema.get(1).name());
    }

    public void testFirstFileWinsIgnoresMismatch() throws Exception {
        List<Attribute> schema1 = List.of(attr("a", DataType.KEYWORD), attr("b", DataType.INTEGER));
        List<Attribute> schema2 = List.of(attr("a", DataType.KEYWORD), attr("b", DataType.INTEGER), attr("c", DataType.LONG));
        List<Attribute> schema3 = List.of(attr("a", DataType.KEYWORD));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/f1.parquet", schema1);
        schemasByPath.put("s3://bucket/data/f2.parquet", schema2);
        schemasByPath.put("s3://bucket/data/f3.parquet", schema3);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(
                entry("s3://bucket/data/f1.parquet", 10),
                entry("s3://bucket/data/f2.parquet", 20),
                entry("s3://bucket/data/f3.parquet", 30)
            )
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        List<Attribute> resolvedSchema = resolved.metadata().schema();
        assertEquals(2, resolvedSchema.size());
        assertEquals("a", resolvedSchema.get(0).name());
        assertEquals("b", resolvedSchema.get(1).name());
    }

    public void testFirstFileWinsSingleFile() throws Exception {
        List<Attribute> schema = List.of(attr("id", DataType.LONG), attr("value", DataType.DOUBLE));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/only.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/data/only.parquet", 500))
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        List<Attribute> resolvedSchema = resolved.metadata().schema();
        assertEquals(2, resolvedSchema.size());
        assertEquals("id", resolvedSchema.get(0).name());
        assertEquals("value", resolvedSchema.get(1).name());
    }

    // ===== Stats partial flag tests =====

    public void testMultiFileFirstFileWinsSetsStatsPartial() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/data/a.parquet", 100), entry("s3://bucket/data/b.parquet", 200))
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        assertEquals(Boolean.TRUE, resolved.metadata().sourceMetadata().get(SourceStatisticsSerializer.STATS_PARTIAL));
    }

    public void testMultiFileFirstFileWinsSetsFileCount() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);
        schemasByPath.put("s3://bucket/data/c.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(
                entry("s3://bucket/data/a.parquet", 100),
                entry("s3://bucket/data/b.parquet", 200),
                entry("s3://bucket/data/c.parquet", 300)
            )
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        assertEquals(3L, resolved.metadata().sourceMetadata().get(SourceStatisticsSerializer.STATS_FILE_COUNT));
    }

    public void testSingleFileFirstFileWinsDoesNotSetStatsPartial() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/only.parquet", schema);

        ExternalSourceResolution resolution = resolveMultiFile(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            List.of(entry("s3://bucket/data/only.parquet", 100))
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        assertNull(resolved.metadata().sourceMetadata().get(SourceStatisticsSerializer.STATS_PARTIAL));
    }

    /**
     * When all files provide per-file row counts, multi-file FIRST_FILE_WINS resolution
     * should aggregate statistics and NOT set STATS_PARTIAL.
     */
    public void testMultiFileFirstFileWinsAggregatesRowCountWhenStatsAvailable() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);
        schemasByPath.put("s3://bucket/data/c.parquet", schema);

        Map<String, Long> rowCountsByPath = new HashMap<>();
        rowCountsByPath.put("s3://bucket/data/a.parquet", 1000L);
        rowCountsByPath.put("s3://bucket/data/b.parquet", 2000L);
        rowCountsByPath.put("s3://bucket/data/c.parquet", 3000L);

        ExternalSourceResolution resolution = resolveMultiFileWithStats(
            "s3://bucket/data/*.parquet",
            schemasByPath,
            rowCountsByPath,
            List.of(
                entry("s3://bucket/data/a.parquet", 100),
                entry("s3://bucket/data/b.parquet", 200),
                entry("s3://bucket/data/c.parquet", 300)
            )
        );

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        Map<String, Object> meta = resolved.metadata().sourceMetadata();
        // Aggregated row count should be the sum across all files.
        assertEquals(6000L, meta.get(SourceStatisticsSerializer.STATS_ROW_COUNT));
        // No STATS_PARTIAL — stats cover all files.
        assertNull(meta.get(SourceStatisticsSerializer.STATS_PARTIAL));
        // File count should still be present.
        assertEquals(3L, meta.get(SourceStatisticsSerializer.STATS_FILE_COUNT));
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

    // ===== Schema type preservation =====

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

    public void testDefaultSchemaResolutionIsFirstFileWins() {
        FormatReader reader = new StubFormatReader(Map.of());
        assertEquals(FormatReader.SchemaResolution.FIRST_FILE_WINS, reader.defaultSchemaResolution());
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
    }

    public void testEnrichSchemaWithPartitionColumnsDirectly() {
        List<Attribute> originalSchema = List.of(attr("a", DataType.INTEGER), attr("b", DataType.KEYWORD));
        ExternalSourceMetadata metadata = createStubMetadata("s3://bucket/file.parquet", originalSchema);

        java.util.LinkedHashMap<String, DataType> partCols = new java.util.LinkedHashMap<>();
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
        assertTrue(schema.get(2).synthetic());
        assertTrue(schema.get(3).synthetic());
    }

    public void testSchemaWithFieldAttributeFailsValidation() throws Exception {
        List<Attribute> schemaWithFieldAttr = List.of(
            new FieldAttribute(Source.EMPTY, "a", new EsField("a", DataType.INTEGER, Map.of(), false, EsField.TimeSeriesFieldType.NONE))
        );
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/file.parquet", schemaWithFieldAttr);

        Map<String, List<StorageEntry>> listingsByPrefix = new HashMap<>();
        listingsByPrefix.put("s3://bucket/data/", List.of(entry("s3://bucket/data/file.parquet", 100)));

        Exception e = expectThrows(
            Exception.class,
            () -> resolveMultiplePaths(List.of("s3://bucket/data/file.parquet"), schemasByPath, listingsByPrefix)
        );
        assertThat(e.getCause().getMessage(), containsString("ReferenceAttribute"));
        assertThat(e.getCause().getMessage(), containsString("FieldAttribute"));
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

        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(settings)) {
            ExternalSourceResolver resolver = createResolverWithCache(countingProvider, schemasByPath, cacheService);

            PlainActionFuture<ExternalSourceResolution> f1 = new PlainActionFuture<>();
            resolver.resolve(List.of("s3://bucket/data/*.parquet"), Map.of(), f1);
            ExternalSourceResolution res1 = f1.actionGet();
            assertNotNull(res1.resolvedSource("s3://bucket/data/*.parquet"));
            assertEquals(2, res1.resolvedSource("s3://bucket/data/*.parquet").fileList().fileCount());
            int listCallsAfterFirst = countingProvider.listCallCount.get();
            int schemaCallsAfterFirst = countingProvider.schemaCallCount.get();
            assertTrue("listing loader should have been called at least once", listCallsAfterFirst > 0);
            assertTrue("schema loader should have been called at least once", schemaCallsAfterFirst > 0);

            PlainActionFuture<ExternalSourceResolution> f2 = new PlainActionFuture<>();
            resolver.resolve(List.of("s3://bucket/data/*.parquet"), Map.of(), f2);
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

        Map<String, Map<String, Object>> pathConfigs = new HashMap<>();
        if (config.isEmpty() == false) {
            pathConfigs.put(globPattern, new HashMap<>(config));
        }

        resolver.resolve(List.of(globPattern), pathConfigs, future);
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
                return Map.of("s3", s -> storageProvider);
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
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        ExternalSourceResolver resolver = new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module);
        PlainActionFuture<ExternalSourceResolution> future = new PlainActionFuture<>();
        resolver.resolve(List.of(globPattern), Map.of(), future);
        return future.actionGet();
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
                return Map.of("s3", s -> storageProvider);
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
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module);
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
                return Map.of("s3", s -> storageProvider);
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
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module, Settings.EMPTY, cacheService);
    }

    // ===== Stub implementations =====

    private static class StubFormatReader implements FormatReader {
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
    private static class StubFormatReaderWithStats implements FormatReader {
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
