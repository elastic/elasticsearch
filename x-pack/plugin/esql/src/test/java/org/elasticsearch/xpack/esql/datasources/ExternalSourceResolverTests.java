/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.SourceMetadata;
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
        blockFactory = new BlockFactory(new NoopCircuitBreaker("test"), BigArrays.NON_RECYCLING_INSTANCE);
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

    // ===== FileSet threading tests =====

    public void testMultiFileResolutionReturnsFileSet() throws Exception {
        List<Attribute> schema = List.of(attr("x", DataType.INTEGER));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/a.parquet", schema);
        schemasByPath.put("s3://bucket/data/b.parquet", schema);

        List<StorageEntry> entries = List.of(entry("s3://bucket/data/a.parquet", 100), entry("s3://bucket/data/b.parquet", 200));

        ExternalSourceResolution resolution = resolveMultiFile("s3://bucket/data/*.parquet", schemasByPath, entries);

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/*.parquet");
        assertNotNull(resolved);
        FileSet fileSet = resolved.fileSet();
        assertTrue(fileSet.isResolved());
        assertEquals(2, fileSet.size());
        assertEquals("s3://bucket/data/a.parquet", fileSet.files().get(0).path().toString());
        assertEquals("s3://bucket/data/b.parquet", fileSet.files().get(1).path().toString());
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
        assertEquals("s3://bucket/dir/*.parquet", resolved.fileSet().originalPattern());
    }

    public void testGlobNoMatchThrows() {
        Map<String, List<Attribute>> schemasByPath = new HashMap<>();

        Exception e = expectThrows(RuntimeException.class, () -> resolveMultiFile("s3://bucket/data/*.parquet", schemasByPath, List.of()));
        assertTrue(e.getMessage().contains("Glob pattern matched no files"));
    }

    // ===== Single-file resolution returns UNRESOLVED FileSet =====

    public void testSingleFileResolutionReturnsUnresolvedFileSet() throws Exception {
        List<Attribute> schema = List.of(attr("id", DataType.LONG));

        Map<String, List<Attribute>> schemasByPath = new HashMap<>();
        schemasByPath.put("s3://bucket/data/single.parquet", schema);

        ExternalSourceResolution resolution = resolveSingleFile("s3://bucket/data/single.parquet", schemasByPath);

        ExternalSourceResolution.ResolvedSource resolved = resolution.resolvedSource("s3://bucket/data/single.parquet");
        assertNotNull(resolved);
        assertTrue(resolved.fileSet().isUnresolved());
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

    // ===== Helpers =====

    private static Attribute attr(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), false, EsField.TimeSeriesFieldType.NONE));
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

        Map<String, Map<String, Expression>> pathParams = new HashMap<>();
        if (config.isEmpty() == false) {
            Map<String, Expression> exprParams = new HashMap<>();
            for (Map.Entry<String, Object> e : config.entrySet()) {
                exprParams.put(e.getKey(), new Literal(Source.EMPTY, new BytesRef(e.getValue().toString()), DataType.KEYWORD));
            }
            pathParams.put(globPattern, exprParams);
        }

        resolver.resolve(List.of(globPattern), pathParams, future);
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
            public Map<String, StorageProviderFactory> storageProviders(Settings settings) {
                return Map.of("s3", s -> storageProvider);
            }

            @Override
            public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
                return Map.of("parquet", (s, bf) -> formatReader);
            }
        };

        DataSourceModule module = new DataSourceModule(List.of(plugin), Settings.EMPTY, blockFactory, EsExecutors.DIRECT_EXECUTOR_SERVICE);

        return new ExternalSourceResolver(EsExecutors.DIRECT_EXECUTOR_SERVICE, module);
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
        public CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize) {
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
}
