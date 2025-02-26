/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

public class TestMapperServiceBuilder {
    private IndexVersion indexVersion;
    private Settings settings;
    private BooleanSupplier idFieldDataEnabled;
    private MapperMetrics mapperMetrics;
    private boolean applyDefaultMapping;
    private XContentBuilder mapping;

    public TestMapperServiceBuilder() {
        indexVersion = IndexVersion.current();
        settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        idFieldDataEnabled = () -> true;
        mapperMetrics = MapperMetrics.NOOP;
        applyDefaultMapping = true;
        mapping = null;
    }

    public TestMapperServiceBuilder indexVersion(IndexVersion indexVersion) {
        this.indexVersion = indexVersion;
        return this;
    }

    public TestMapperServiceBuilder withSettings(Settings settings) {
        this.settings = settings;
        return this;
    }

    public TestMapperServiceBuilder idFieldDataEnabled(BooleanSupplier idFieldDataEnabled) {
        this.idFieldDataEnabled = idFieldDataEnabled;
        return this;
    }

    public TestMapperServiceBuilder mapperMetrics(MapperMetrics mapperMetrics) {
        this.mapperMetrics = mapperMetrics;
        return this;
    }

    public TestMapperServiceBuilder applyDefaultMapping(boolean applyDefaultMapping) {
        this.applyDefaultMapping = applyDefaultMapping;
        return this;
    }

    public TestMapperServiceBuilder withMapping(XContentBuilder mappingXContent) {
        this.mapping = mappingXContent;
        return this;
    }

    public MapperService build() throws IOException {
        var settingsWithMoreDefaults = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(settings)
            .put(IndexMetadata.SETTING_VERSION_CREATED, indexVersion)
            .build();
        IndexMetadata meta = IndexMetadata.builder("index").settings(settingsWithMoreDefaults).build();
        var indexSettings = new IndexSettings(meta, settingsWithMoreDefaults);

        SimilarityService similarityService = new SimilarityService(indexSettings, null, Map.of());
        // TODO support plugins
        MapperRegistry mapperRegistry = new IndicesModule(List.of()).getMapperRegistry();

        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, BitsetFilterCache.Listener.NOOP);

        var indexAnalyzers = IndexAnalyzers.of(
            Map.of("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer()))
        );

        var xContentRegistry = new NamedXContentRegistry(
            CollectionUtils.concatLists(ClusterModule.getNamedXWriteables(), IndicesModule.getNamedXContents())
        );
        XContentParserConfiguration parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        var mapperService = new MapperService(
            TransportVersion::current,
            indexSettings,
            indexAnalyzers,
            parserConfig,
            similarityService,
            mapperRegistry,
            () -> {
                throw new UnsupportedOperationException();
            },
            indexSettings.getMode().buildIdFieldMapper(idFieldDataEnabled),
            this::compileScript,
            bitsetFilterCache::getBitSetProducer,
            mapperMetrics
        );

        if (applyDefaultMapping && indexSettings.getMode().getDefaultMapping(indexSettings) != null) {
            mapperService.merge(null, indexSettings.getMode().getDefaultMapping(indexSettings), MapperService.MergeReason.MAPPING_UPDATE);
        }

        if (mapping != null) {
            mapperService.merge(null, new CompressedXContent(BytesReference.bytes(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
        }

        return mapperService;
    }

    private <T> T compileScript(Script script, ScriptContext<T> context) {
        throw new UnsupportedOperationException("Script compilation is not supported");
    }
}
