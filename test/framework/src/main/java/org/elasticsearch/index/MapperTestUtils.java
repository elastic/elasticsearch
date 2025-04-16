/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.cache.bitset.BitsetFilterCache;
import org.elasticsearch.index.mapper.MapperMetrics;
import org.elasticsearch.index.mapper.MapperRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.createTestAnalysis;

public class MapperTestUtils {
    public static MapperService newMapperService(
        NamedXContentRegistry xContentRegistry,
        Path tempDir,
        Settings indexSettings,
        String indexName
    ) throws IOException {
        return newMapperService(List.of(), xContentRegistry, tempDir, indexSettings, indexName);
    }

    public static MapperService newMapperService(
        List<MapperPlugin> extraMappers,
        NamedXContentRegistry xContentRegistry,
        Path tempDir,
        Settings indexSettings,
        String indexName
    ) throws IOException {
        IndicesModule indicesModule = new IndicesModule(extraMappers);
        return newMapperService(xContentRegistry, tempDir, indexSettings, indicesModule, indexName);
    }

    public static MapperService newMapperService(
        NamedXContentRegistry xContentRegistry,
        Path tempDir,
        Settings settings,
        IndicesModule indicesModule,
        String indexName
    ) throws IOException {
        Settings.Builder settingsBuilder = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), tempDir).put(settings);
        if (settings.get(IndexMetadata.SETTING_VERSION_CREATED) == null) {
            settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
        }
        Settings finalSettings = settingsBuilder.build();
        MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(indexName, finalSettings);
        IndexAnalyzers indexAnalyzers = createTestAnalysis(indexSettings, finalSettings).indexAnalyzers;
        SimilarityService similarityService = new SimilarityService(indexSettings, null, Collections.emptyMap());
        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, BitsetFilterCache.Listener.NOOP);
        return new MapperService(
            () -> TransportVersion.current(),
            indexSettings,
            indexAnalyzers,
            XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry).withDeprecationHandler(LoggingDeprecationHandler.INSTANCE),
            similarityService,
            mapperRegistry,
            () -> null,
            indexSettings.getMode().idFieldMapperWithoutFieldData(),
            ScriptCompiler.NONE,
            bitsetFilterCache::getBitSetProducer,
            MapperMetrics.NOOP
        );
    }
}
