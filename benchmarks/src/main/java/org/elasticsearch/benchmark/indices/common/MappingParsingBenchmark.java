/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.indices.common;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class MappingParsingBenchmark {
    static {
        // For Elasticsearch900Lucene101Codec:
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
        LogConfigurator.setNodeName("test");
    }

    private static final String MAPPING = """
        {
          "_doc": {
            "dynamic": false,
            "properties": {
              "@timestamp": {
                "type": "date"
              },
              "host": {
                "type": "object",
                "properties": {
                  "name": {
                    "type": "keyword"
                  }
                }
              },
              "message": {
                "type": "pattern_text"
              }
            }
          }
        }
        \s""";

    @Param("1024")
    private int numIndices;

    private List<MapperService> mapperServices;
    private CompressedXContent compressedMapping;

    private Random random = new Random();
    private static final String CHARS = "abcdefghijklmnopqrstuvwxyz1234567890";

    private String randomIndexName() {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            b.append(CHARS.charAt(random.nextInt(CHARS.length())));
        }
        return b.toString();
    }

    @Setup
    public void setUp() throws IOException {
        Settings settings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", 1)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put("index.mode", "logsdb")
            .put("index.logsdb.sort_on_host_name", true)
            .put("index.logsdb.sort_on_message_template", true)
            .build();

        LogsDBPlugin logsDBPlugin = new LogsDBPlugin(settings);

        Set<Setting<?>> definedSettings = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        definedSettings.addAll(logsDBPlugin.getSettings().stream().filter(Setting::hasIndexScope).toList());
        IndexScopedSettings indexScopedSettings = new IndexScopedSettings(Settings.EMPTY, definedSettings);

        mapperServices = new ArrayList<>(numIndices);
        for (int i = 0; i < numIndices; i++) {
            IndexMetadata meta = IndexMetadata.builder(randomIndexName()).settings(settings).build();
            IndexSettings indexSettings = new IndexSettings(meta, settings, indexScopedSettings);
            MapperRegistry mapperRegistry = new IndicesModule(List.of(logsDBPlugin)).getMapperRegistry();
            SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
            BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, BitsetFilterCache.Listener.NOOP);
            MapperService mapperService = new MapperService(
                TransportVersion::current,
                indexSettings,
                IndexAnalyzers.of(Map.of()),
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
                MapperMetrics.NOOP,
                null,
                null
            );

            mapperServices.add(mapperService);
        }

        compressedMapping = new CompressedXContent(MAPPING);
    }

    @Benchmark
    public void mappingParsingBenchmark() {
        for (MapperService service : mapperServices) {
            service.merge("_doc", compressedMapping, MapperService.MergeReason.MAPPING_UPDATE);
        }
    }
}
