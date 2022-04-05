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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Fork(value = 1) // jvmArgs = { "-XX:+PrintCompilation", "-XX:+UnlockDiagnosticVMOptions", "-XX:+PrintInlining" })
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class KeywordFieldMapperBenchmark {

    private MapperService mapperService;

    private SourceToParse sourceToParse;

    @Setup
    public void setUp() {
        this.mapperService = createMapperService("""
            {
              "_doc": {
                "dynamic": false,
                "properties": {
                  "host": {
                    "type": "keyword",
                    "store": true
                  },
                  "name": {
                    "type": "keyword"
                  },
                  "type": {
                    "type": "keyword",
                    "normalizer": "lowercase"
                  },
                  "uuid": {
                    "type": "keyword",
                    "doc_values": false
                  },
                  "version": {
                    "type": "keyword"
                  },
                  "extra_field": {
                    "type": "keyword"
                  },
                  "extra_field_text": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 1024
                      }
                    }
                  },
                  "nested_object": {
                    "properties": {
                      "keyword_nested": {
                        "type": "keyword"
                      },
                      "text_nested": {
                        "type": "text"
                      },
                      "number_nested": {
                        "type": "long"
                      }
                    }
                  }
                }
              }
            }
                       \s""");
        this.sourceToParse = new SourceToParse(UUIDs.randomBase64UUID(), new BytesArray("""
            {
              "host": "some_value",
              "name": "some_other_thing",
              "type": "some_type_thing",
              "uuid": "some_keyword_uuid",
              "version": "some_version",
              "extra_field_text": "bla bla text",
              "nested_object" : {
                "keyword_nested": "some random nested keyword",
                "text_nested": "some random nested text",
                "number_nested": 123234234
              }
            }"""), XContentType.JSON);
    }

    private static MapperService createMapperService(String mappings) {
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
        return mapperService.documentMapper().parse(sourceToParse).docs();
    }
}
