/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.LowercaseNormalizer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Fork(value = 1) // jvmArgs = { "-XX:+PrintCompilation", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining" })
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class BeatsMapperBenchmark {

    private MapperService mapperService;
    private SourceToParse[] sources;
    private Random random;

    @Setup
    public void setUp() throws IOException, URISyntaxException {
        this.mapperService = createMapperService(readSampleMapping());
        this.sources = readSampleDocuments();
        this.random = new Random();
    }

    private static String readSampleMapping() throws IOException, URISyntaxException {
        return Files.readString(pathToResource("filebeat-mapping-8.1.2.json"));
    }

    private static SourceToParse[] readSampleDocuments() throws IOException, URISyntaxException {
        return Files.readAllLines(pathToResource("sample_documents.json"))
            .stream()
            .map(source -> new SourceToParse(UUIDs.randomBase64UUID(), new BytesArray(source), XContentType.JSON))
            .toArray(SourceToParse[]::new);
    }

    private static Path pathToResource(String path) throws URISyntaxException {
        return Paths.get(BeatsMapperBenchmark.class.getResource(path).toURI());
    }

    private static MapperService createMapperService(String mappings) {
        Settings settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put("index.version.created", Version.CURRENT)
            .put("index.mapping.total_fields.limit", 10000)
            .build();
        IndexMetadata meta = IndexMetadata.builder("index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(meta, settings);
        MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();

        SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        MapperService mapperService = new MapperService(
            indexSettings,
            new IndexAnalyzers(
                Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
                Map.of("lowercase", new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())),
                Map.of()
            ),
            XContentParserConfiguration.EMPTY.withRegistry(new NamedXContentRegistry(ClusterModule.getNamedXWriteables()))
                .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
            similarityService,
            mapperRegistry,
            () -> { throw new UnsupportedOperationException(); },
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

    @Benchmark
    public List<LuceneDocument> benchmarkParseKeywordFields() {
        var source = sources[random.nextInt(sources.length)];
        return mapperService.documentMapper().parse(source).docs();
    }
}
