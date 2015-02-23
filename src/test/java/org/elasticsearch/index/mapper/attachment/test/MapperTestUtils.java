/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper.attachment.test;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.analysis.AnalysisModule;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.index.similarity.SimilarityLookupService;
import org.elasticsearch.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCacheListener;
import org.elasticsearch.threadpool.ThreadPool;

public class MapperTestUtils {

    public static MapperService newMapperService(ThreadPool testingThreadPool) {
        return newMapperService(new Index("test"), ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build(), testingThreadPool);
    }

    public static MapperService newMapperService(Index index, Settings indexSettings, ThreadPool testingThreadPool) {
        NoneCircuitBreakerService circuitBreakerService = new NoneCircuitBreakerService();
        return new MapperService(index, indexSettings, new Environment(), newAnalysisService(), new IndexFieldDataService(index, ImmutableSettings.Builder.EMPTY_SETTINGS,
                new IndicesFieldDataCache(ImmutableSettings.Builder.EMPTY_SETTINGS, new IndicesFieldDataCacheListener(circuitBreakerService), testingThreadPool),
                circuitBreakerService), newSimilarityLookupService(), null);
    }

    public static AnalysisService newAnalysisService() {
        return newAnalysisService(ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build());
    }

    public static AnalysisService newAnalysisService(Settings indexSettings) {
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(indexSettings), new EnvironmentModule(new Environment(ImmutableSettings.Builder.EMPTY_SETTINGS)), new IndicesAnalysisModule()).createInjector();
        Index index = new Index("test");
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(index, indexSettings),
                new IndexNameModule(index),
                new AnalysisModule(indexSettings, parentInjector.getInstance(IndicesAnalysisService.class))).createChildInjector(parentInjector);

        return injector.getInstance(AnalysisService.class);
    }

    public static SimilarityLookupService newSimilarityLookupService() {
        return new SimilarityLookupService(new Index("test"), ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public static DocumentMapperParser newMapperParser() {
        return newMapperParser(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public static DocumentMapperParser newMapperParser(Settings settings) {
        Settings forcedSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(settings)
                .build();
        return new DocumentMapperParser(new Index("test"), forcedSettings, MapperTestUtils.newAnalysisService(forcedSettings), null, null);
    }
}
