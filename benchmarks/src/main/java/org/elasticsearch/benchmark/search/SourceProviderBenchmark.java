/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class SourceProviderBenchmark {
    static {
        LogConfigurator.setNodeName("test");
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
    }

    private Directory directory;
    private DirectoryReader indexReader;
    private MapperService mapperService;
    private CodecService codecService;
    private int rootDoc;

    @Param({ "patch", "synthetic", "stored", "exclude" })
    private String mode;

    @Param({ "small", "medium", "large" })
    private String docSize;

    @Setup
    public void setup() throws IOException {
        this.mapperService = createMapperService(readMapping(mode).utf8ToString());
        this.codecService = new CodecService(mapperService, BigArrays.NON_RECYCLING_INSTANCE);

        IndexWriterConfig iwc = new IndexWriterConfig(IndexShard.buildIndexAnalyzer(mapperService));
        iwc.setCodec(codecService.codec(CodecService.DEFAULT_CODEC));
        this.directory = new MMapDirectory(Files.createTempDirectory("sourceLoaderBench"));
        try (IndexWriter iw = new IndexWriter(directory, iwc)) {
            var bytes = readWikipediaDocument(docSize);
            var source = new SourceToParse("0", bytes, XContentType.JSON);
            ParsedDocument doc = mapperService.documentMapper().parse(source);
            assert doc.dynamicMappingsUpdate() == null;
            iw.addDocuments(doc.docs());
            rootDoc = doc.docs().size() - 1;
            iw.commit();
        }

        this.indexReader = ElasticsearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId(new Index("index", "_na_"), 0));
    }

    @TearDown
    public void tearDown() {
        IOUtils.closeWhileHandlingException(indexReader, directory);
    }

    @Benchmark
    public void loadDoc() throws IOException {
        var provider = SourceProvider.fromLookup(
            null,
            mapperService.mappingLookup(),
            mapperService.getMapperMetrics().sourceFieldMetrics()
        );
        var source = provider.getSource(indexReader.leaves().get(0), rootDoc);
        assert source.internalSourceRef() != null;
    }

    @Benchmark
    public void loadFilterDoc() throws IOException {
        var provider = SourceProvider.fromLookup(
            mode.equals("exclude") ? null : new SourceFilter(null, new String[] { "chunks.emb" }),
            mapperService.mappingLookup(),
            mapperService.getMapperMetrics().sourceFieldMetrics()
        );
        var source = provider.getSource(indexReader.leaves().get(0), rootDoc);
        assert source.internalSourceRef() != null;
    }

    protected final MapperService createMapperService(String mappings) {
        Settings settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .build();
        IndexMetadata meta = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(meta, settings);
        MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();

        SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {}

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {}
        });
        MapperService mapperService = new MapperService(
            () -> TransportVersion.current(),
            indexSettings,
            (type, name) -> Lucene.STANDARD_ANALYZER,
            XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()))
                .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
            similarityService,
            mapperRegistry,
            () -> {
                throw new UnsupportedOperationException();
            },
            new ProvidedIdFieldMapper(() -> true),
            new ScriptCompiler() {
                @Override
                public <T> T compile(Script script, ScriptContext<T> scriptContext) {
                    throw new UnsupportedOperationException();
                }
            },
            bitsetFilterCache::getBitSetProducer,
            MapperMetrics.NOOP
        );

        try {
            mapperService.merge("_doc", new CompressedXContent(mappings), MapperService.MergeReason.MAPPING_UPDATE);
            return mapperService;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private BytesReference readMapping(String mode) throws IOException {
        return Streams.readFully(SourceProviderBenchmark.class.getResourceAsStream("wikipedia_mapping_" + mode + ".json"));
    }

    private BytesReference readWikipediaDocument(String docSize) throws IOException {
        return Streams.readFully(SourceProviderBenchmark.class.getResourceAsStream("wikipedia_" + docSize + ".json"));
    }
}
