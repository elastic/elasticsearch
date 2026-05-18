/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.codec.PerFieldMapperCodec;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class TimeSeriesMetadataFieldBlockLoaderTests extends MapperServiceTestCase {

    /**
     * Plain TSDB index. {@link IndexMode#TIME_SERIES} defaults {@code _source.mode} to
     * {@link SourceFieldMapper.Mode#SYNTHETIC}, so synthetic source reconstructs {@code _source} as JSON.
     */
    private static final Settings TSDB_SYNTHETIC_SETTINGS = tsdbSettings(null, "host");

    /**
     * TSDB index with {@code index.mapping.source.mode: stored}. Stored source preserves the raw
     * indexed bytes (e.g. CBOR for Prometheus remote-write documents), so the {@code _timeseries} loader must explicitly normalize to JSON.
     */
    private static final Settings TSDB_STORED_SETTINGS = tsdbSettings(SourceFieldMapper.Mode.STORED, "host");

    /**
     * TSDB index that mirrors the Prometheus mapping: a {@code labels} {@code passthrough} object marked as
     * {@code time_series_dimension: true}. Dimension fields show up under {@code labels.*} and the index uses stored source.
     */
    private static final Settings TSDB_PROMETHEUS_LIKE_SETTINGS = tsdbSettings(SourceFieldMapper.Mode.STORED, "labels.*");

    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "@timestamp": { "type": "date" },
              "host": { "type": "keyword", "time_series_dimension": true },
              "env": { "type": "keyword", "time_series_dimension": true },
              "region": { "type": "keyword", "time_series_dimension": true },
              "cpu": { "type": "double", "time_series_metric": "gauge" },
              "request_count": { "type": "long", "time_series_metric": "counter" },
              "message": { "type": "keyword" }
            }
          }
        }
        """;

    private static final String PROMETHEUS_LIKE_MAPPING = """
        {
          "_doc": {
            "properties": {
              "@timestamp": { "type": "date" },
              "labels": {
                "type": "passthrough",
                "priority": 10,
                "time_series_dimension": true,
                "properties": {
                  "__name__": { "type": "keyword" },
                  "instance": { "type": "keyword" },
                  "job": { "type": "keyword" }
                }
              },
              "metrics": {
                "type": "passthrough",
                "priority": 20,
                "properties": {
                  "go_gc_cleanups_executed_cleanups_total": { "type": "double", "time_series_metric": "counter" }
                }
              }
            }
          }
        }
        """;

    private static Settings tsdbSettings(SourceFieldMapper.Mode sourceMode, String routingPath) {
        Settings.Builder builder = Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z");
        if (sourceMode != null) {
            builder.put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode);
        }
        return builder.build();
    }

    public void testDimensionsOnly() throws IOException {
        BlockLoader loader = createBlockLoader(
            TSDB_SYNTHETIC_SETTINGS,
            MAPPING,
            new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of())
        );
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        assertThat(sourcePaths(loader), equalTo(Set.of("host", "env", "region")));
    }

    public void testDimensionsAndMetrics() throws IOException {
        BlockLoader loader = createBlockLoader(
            TSDB_SYNTHETIC_SETTINGS,
            MAPPING,
            new BlockLoaderFunctionConfig.TimeSeriesMetadata(true, Set.of())
        );
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        assertThat(sourcePaths(loader), equalTo(Set.of("host", "env", "region", "cpu", "request_count")));
    }

    public void testExcludedDimensions() throws IOException {
        BlockLoader loader = createBlockLoader(
            TSDB_SYNTHETIC_SETTINGS,
            MAPPING,
            new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of("host", "region"))
        );
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        assertThat(sourcePaths(loader), equalTo(Set.of("env")));
    }

    public void testExcludedDimensionsWithMetrics() throws IOException {
        BlockLoader loader = createBlockLoader(
            TSDB_SYNTHETIC_SETTINGS,
            MAPPING,
            new BlockLoaderFunctionConfig.TimeSeriesMetadata(true, Set.of("env"))
        );
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));
        assertThat(sourcePaths(loader), equalTo(Set.of("host", "region", "cpu", "request_count")));
    }

    public void testNoConfigReturnSourceBlockLoader() throws IOException {
        MapperService mapperService = createMapperService(TSDB_SYNTHETIC_SETTINGS, MAPPING);
        BlockLoader loader = mapperService.documentMapper()
            .sourceMapper()
            .fieldType()
            .blockLoader(new TestBlockLoaderContext(mapperService, null));
        assertThat(loader, instanceOf(SourceFieldBlockLoader.class));
    }

    /**
     * Synthetic source TSDB indices reconstruct {@code _source} as JSON, so the loader can stream the bytes
     * straight into the keyword block. The output must still be a valid JSON object that contains every
     * dimension on the document.
     */
    public void testReadEmitsJsonForSyntheticSourceIndex() throws IOException {
        BytesReference json = bytes(XContentType.JSON, b -> writeTimestampAndDimensions(b, "host-1", "prod", "us-east-1"));
        BytesRef value = readTimeSeriesValue(TSDB_SYNTHETIC_SETTINGS, MAPPING, sourceToParse(json, XContentType.JSON));
        assertThat(parseJsonObject(value), equalTo(Map.of("host", "host-1", "env", "prod", "region", "us-east-1")));
    }

    /**
     * Stored source preserves the original encoding. The Prometheus remote-write endpoint indexes documents
     * via {@link XContentFactory#cborBuilder()}, so a {@code stored} TSDB index ends up with CBOR bytes on
     * disk. The loader must convert the bytes to JSON before populating the keyword block; otherwise users
     * see binary garbage in the {@code _timeseries} column.
     */
    public void testReadConvertsCborStoredSourceToJson() throws IOException {
        BytesReference cbor = bytes(XContentType.CBOR, b -> writeTimestampAndDimensions(b, "host-1", "prod", "us-east-1"));
        BytesRef value = readTimeSeriesValue(TSDB_STORED_SETTINGS, MAPPING, sourceToParse(cbor, XContentType.CBOR));
        assertThat(parseJsonObject(value), equalTo(Map.of("host", "host-1", "env", "prod", "region", "us-east-1")));
    }

    /**
     * Stored JSON source already satisfies the contract. The loader should still emit the bytes as a JSON
     * keyword without corrupting them.
     */
    public void testReadPassesThroughJsonStoredSource() throws IOException {
        BytesReference json = bytes(XContentType.JSON, b -> writeTimestampAndDimensions(b, "host-1", "prod", "us-east-1"));
        BytesRef value = readTimeSeriesValue(TSDB_STORED_SETTINGS, MAPPING, sourceToParse(json, XContentType.JSON));
        assertThat(parseJsonObject(value), equalTo(Map.of("host", "host-1", "env", "prod", "region", "us-east-1")));
    }

    /**
     * A TSDB index modeled after the Prometheus remote-write component template, with a {@code labels} {@code passthrough} object marked as
     * a dimension and stored CBOR source. The emitted {@code _timeseries} JSON object must contain a nested {@code labels} object with
     * the indexed dimension key/value pairs.
     */
    public void testReadJsonContainsDimensionKeyValuePairs() throws IOException {
        BytesReference cbor = bytes(XContentType.CBOR, b -> {
            b.field("@timestamp", "2021-04-28T18:50:00Z");
            b.startObject("labels");
            b.field("__name__", "go_gc_cleanups_executed_cleanups_total");
            b.field("instance", "localhost:9090");
            b.field("job", "prometheus");
            b.endObject();
            b.startObject("metrics");
            b.field("go_gc_cleanups_executed_cleanups_total", 1.0);
            b.endObject();
        });
        BytesRef value = readTimeSeriesValue(
            TSDB_PROMETHEUS_LIKE_SETTINGS,
            PROMETHEUS_LIKE_MAPPING,
            sourceToParse(cbor, XContentType.CBOR)
        );
        Map<String, Object> parsed = parseJsonObject(value);
        assertThat(parsed.keySet(), equalTo(Set.of("labels")));
        @SuppressWarnings("unchecked")
        Map<String, Object> labels = (Map<String, Object>) parsed.get("labels");
        assertThat(
            labels,
            equalTo(Map.of("__name__", "go_gc_cleanups_executed_cleanups_total", "instance", "localhost:9090", "job", "prometheus"))
        );
    }

    /**
     * Build the {@code _timeseries} block loader, index a single document, and return the {@link BytesRef}
     * the loader writes into a one-row block. This mirrors how the production code wires
     * {@link TimeSeriesMetadataFieldBlockLoader} into a row-stride read at search time.
     */
    private BytesRef readTimeSeriesValue(Settings settings, String mapping, SourceToParse sourceToParse) throws IOException {
        MapperService mapperService = createMapperService(settings, mapping);
        BlockLoader loader = mapperService.documentMapper()
            .sourceMapper()
            .fieldType()
            .blockLoader(new TestBlockLoaderContext(mapperService, new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of())));
        assertThat(loader, instanceOf(TimeSeriesMetadataFieldBlockLoader.class));

        AtomicReference<BytesRef> result = new AtomicReference<>();
        withTsdbLuceneIndex(mapperService, writer -> {
            ParsedDocument parsed = mapperService.documentMapper().parse(sourceToParse);
            writer.addDocument(parsed.rootDoc());
        }, (reader, ctx) -> {
            CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
            try (BlockLoader.RowStrideReader rowReader = loader.rowStrideReader(breaker, ctx)) {
                StoredFieldsSpec loaderSpec = loader.rowStrideStoredFieldSpec();
                SourceFilter filter = loaderSpec.requiresSource()
                    ? new SourceFilter(loaderSpec.sourcePaths().toArray(new String[0]), null)
                    : null;
                SourceLoader sourceLoader = mapperService.mappingLookup().newSourceLoader(filter, SourceFieldMetrics.NOOP);
                SourceLoader.Leaf sourceLeaf = sourceLoader.leaf(ctx.reader(), null);
                StoredFieldsSpec storedFieldsSpec = loaderSpec.merge(
                    new StoredFieldsSpec(true, false, sourceLoader.requiredStoredFields())
                );
                BlockLoaderStoredFieldsFromLeafLoader storedFields = new BlockLoaderStoredFieldsFromLeafLoader(
                    StoredFieldLoader.fromSpec(storedFieldsSpec).getLoader(ctx, null),
                    sourceLeaf
                );
                BlockLoader.Builder builder = loader.builder(TestBlock.factory(), 1);
                storedFields.advanceTo(0);
                rowReader.read(0, storedFields, builder);
                TestBlock block = (TestBlock) builder.build();
                assertThat(block.size(), equalTo(1));
                result.set((BytesRef) block.get(0));
            }
        });
        return result.get();
    }

    /**
     * Variant of {@link MapperServiceTestCase#withLuceneIndex} that disables the close-time
     * {@code CheckIndex} run on {@code MockDirectoryWrapper}. Indexing a single TSDB document via
     * {@link RandomIndexWriter} does not exercise the production indexing path that {@code CheckIndex}
     * expects (e.g. {@code _seq_no}, {@code _version} fields written by the engine), so {@code CheckIndex}
     * trips on {@code docfreq: 0} for postings unrelated to what these tests actually exercise. The block
     * loader read path under test only consumes stored {@code _source} bytes, so a clean exit suffices.
     */
    private static void withTsdbLuceneIndex(
        MapperService mapperService,
        CheckedConsumer<RandomIndexWriter, IOException> indexer,
        CheckedBiConsumer<DirectoryReader, LeafReaderContext, IOException> test
    ) throws IOException {
        IndexSortConfig sortConfig = new IndexSortConfig(mapperService.getIndexSettings());
        Sort indexSort = sortConfig.buildIndexSort(
            mapperService::fieldType,
            (ft, s) -> ft.fielddataBuilder(FieldDataContext.noRuntimeFields("index", "")).build(null, null)
        );
        IndexWriterConfig iwc = new IndexWriterConfig(IndexShard.buildIndexAnalyzer(mapperService)).setCodec(
            new PerFieldMapperCodec(Zstd814StoredFieldsFormat.Mode.BEST_SPEED, mapperService, BigArrays.NON_RECYCLING_INSTANCE, null)
        );
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }
        try (Directory dir = newDirectory()) {
            ((BaseDirectoryWrapper) dir).setCheckIndexOnClose(false);
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc)) {
                indexer.accept(iw);
                try (DirectoryReader reader = iw.getReader()) {
                    assertThat(reader.leaves(), hasSize(1));
                    test.accept(reader, reader.leaves().get(0));
                }
            }
        }
    }

    private static void writeTimestampAndDimensions(XContentBuilder builder, String host, String env, String region) throws IOException {
        builder.field("@timestamp", "2021-04-28T18:50:00Z");
        builder.field("host", host);
        builder.field("env", env);
        builder.field("region", region);
    }

    /**
     * Build a {@link SourceToParse} for a single TSDB document. The {@code _id} is left {@code null} so the
     * mapper generates the synthetic {@code _id} from the dimensions and {@code @timestamp}; we pass an
     * explicit routing hash so {@link TimeSeriesRoutingHashFieldMapper} does not fall back to extracting
     * one from a (non-existent) synthetic id.
     */
    private static SourceToParse sourceToParse(BytesReference bytes, XContentType type) {
        return new SourceToParse(null, bytes, type, TimeSeriesRoutingHashFieldMapper.encode(0));
    }

    private static BytesReference bytes(XContentType type, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(type)) {
            builder.startObject();
            body.accept(builder);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    private static Map<String, Object> parseJsonObject(BytesRef value) throws IOException {
        BytesReference asReference = new BytesArray(value.bytes, value.offset, value.length);
        Map<String, Object> raw = XContentHelper.convertToMap(asReference, false, XContentType.JSON).v2();
        return new LinkedHashMap<>(raw);
    }

    private BlockLoader createBlockLoader(Settings settings, String mapping, BlockLoaderFunctionConfig config) throws IOException {
        MapperService mapperService = createMapperService(settings, mapping);
        return mapperService.documentMapper().sourceMapper().fieldType().blockLoader(new TestBlockLoaderContext(mapperService, config));
    }

    private static Set<String> sourcePaths(BlockLoader loader) {
        var spec = loader.rowStrideStoredFieldSpec();
        return spec.requiresSource() ? spec.sourcePaths() : Set.of();
    }

    private static class TestBlockLoaderContext extends DummyBlockLoaderContext.MapperServiceBlockLoaderContext {
        private final BlockLoaderFunctionConfig config;

        private TestBlockLoaderContext(MapperService mapperService, BlockLoaderFunctionConfig config) {
            super(mapperService);
            this.config = config;
        }

        @Override
        public BlockLoaderFunctionConfig blockLoaderFunctionConfig() {
            return config;
        }
    }
}
