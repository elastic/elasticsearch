/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.search;

import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class QueryParserHelperBenchmark {

    private static final int NUMBER_OF_MAPPING_FIELDS = 1000;

    private Directory directory;
    private IndexReader indexReader;
    private MapperService mapperService;

    @Setup
    public void setup() throws IOException {
        // pre: set up MapperService and SearchExecutionContext
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_MAPPING_FIELDS; i++) {
            fields.add(String.format("""
                "field%d":{"type":"long"}""", i));
        }
        String mappings = """
            {"_doc":{"properties":{""" + Strings.join(fields, ',') + "}}}";

        mapperService = createMapperService(mappings);
        IndexWriterConfig iwc = new IndexWriterConfig(IndexShard.buildIndexAnalyzer(mapperService));
        directory = new ByteBuffersDirectory();
        IndexWriter iw = new IndexWriter(directory, iwc);

        for (int i = 0; i < 2000; i++) {
            ParsedDocument doc = mapperService.documentMapper().parse(buildDoc(i));
            iw.addDocument(doc.rootDoc());
            if (i % 100 == 0) {
                iw.commit();
            }
        }
        iw.close();

        indexReader = DirectoryReader.open(directory);
    }

    private SourceToParse buildDoc(int docId) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < NUMBER_OF_MAPPING_FIELDS; i++) {
            if (i % 2 == 0) continue;
            if (i % 3 == 0 && (docId < (NUMBER_OF_MAPPING_FIELDS / 2))) continue;
            fields.add(String.format("""
                "field%d":1""", i));
        }
        String source = "{" + String.join(",", fields) + "}";
        return new SourceToParse("" + docId, new BytesArray(source), XContentType.JSON);
    }

    @TearDown
    public void tearDown() {
        IOUtils.closeWhileHandlingException(indexReader, directory);
    }

    @Benchmark
    public void expand() {
        Map<String, Float> fields = QueryParserHelper.resolveMappingFields(buildSearchExecutionContext(), Map.of("*", 1f));
        assert fields.size() > 0 && fields.size() < NUMBER_OF_MAPPING_FIELDS;
    }

    protected SearchExecutionContext buildSearchExecutionContext() {
        final SimilarityService similarityService = new SimilarityService(mapperService.getIndexSettings(), null, Map.of());
        final long nowInMillis = 1;
        return new SearchExecutionContext(
            0,
            0,
            mapperService.getIndexSettings(),
            null,
            (ft, fdc) -> ft.fielddataBuilder(fdc).build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService()),
            mapperService,
            mapperService.mappingLookup(),
            similarityService,
            null,
            XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()))
                .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
            null,
            new IndexSearcher(indexReader),
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null,
            Collections.emptyMap()
        );
    }

    protected final MapperService createMapperService(String mappings) {
        Settings settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put("index.version.created", Version.CURRENT)
            .build();
        IndexMetadata meta = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(meta, settings);
        MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();

        SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        MapperService mapperService = new MapperService(
            () -> TransportVersion.CURRENT,
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
            }
        );

        try {
            mapperService.merge("_doc", new CompressedXContent(mappings), MapperService.MergeReason.MAPPING_UPDATE);
            return mapperService;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
