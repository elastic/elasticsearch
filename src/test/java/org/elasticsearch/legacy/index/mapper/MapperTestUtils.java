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

package org.elasticsearch.legacy.index.mapper;

import org.elasticsearch.legacy.common.inject.Injector;
import org.elasticsearch.legacy.common.inject.ModulesBuilder;
import org.elasticsearch.legacy.common.settings.ImmutableSettings;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.settings.SettingsModule;
import org.elasticsearch.legacy.env.Environment;
import org.elasticsearch.legacy.env.EnvironmentModule;
import org.elasticsearch.legacy.index.Index;
import org.elasticsearch.legacy.index.IndexNameModule;
import org.elasticsearch.legacy.index.analysis.AnalysisModule;
import org.elasticsearch.legacy.index.analysis.AnalysisService;
import org.elasticsearch.legacy.index.codec.docvaluesformat.DocValuesFormatService;
import org.elasticsearch.legacy.index.codec.postingsformat.PostingsFormatService;
import org.elasticsearch.legacy.index.fielddata.IndexFieldDataService;
import org.elasticsearch.legacy.index.settings.IndexSettingsModule;
import org.elasticsearch.legacy.index.similarity.SimilarityLookupService;
import org.elasticsearch.legacy.indices.analysis.IndicesAnalysisModule;
import org.elasticsearch.legacy.indices.analysis.IndicesAnalysisService;
import org.elasticsearch.legacy.indices.fielddata.breaker.NoneCircuitBreakerService;

/**
 *
 */
public class MapperTestUtils {

    public static DocumentMapperParser newParser() {
        return new DocumentMapperParser(new Index("test"), ImmutableSettings.Builder.EMPTY_SETTINGS, newAnalysisService(), new PostingsFormatService(new Index("test")),
                new DocValuesFormatService(new Index("test")), newSimilarityLookupService(), null);
    }

    public static DocumentMapperParser newParser(Settings indexSettings) {
        return new DocumentMapperParser(new Index("test"), indexSettings, newAnalysisService(indexSettings), new PostingsFormatService(new Index("test")),
                new DocValuesFormatService(new Index("test")), newSimilarityLookupService(), null);
    }

    public static MapperService newMapperService() {
        return newMapperService(new Index("test"), ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public static MapperService newMapperService(Index index, Settings indexSettings) {
        return new MapperService(index, indexSettings, new Environment(), newAnalysisService(), new IndexFieldDataService(index, new NoneCircuitBreakerService()),
                new PostingsFormatService(index), new DocValuesFormatService(index), newSimilarityLookupService(), null);
    }

    public static AnalysisService newAnalysisService() {
        return newAnalysisService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    public static AnalysisService newAnalysisService(Settings indexSettings) {
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(indexSettings), new EnvironmentModule(new Environment(ImmutableSettings.Builder.EMPTY_SETTINGS)), new IndicesAnalysisModule()).createInjector();
        Injector injector = new ModulesBuilder().add(
                new IndexSettingsModule(new Index("test"), indexSettings),
                new IndexNameModule(new Index("test")),
                new AnalysisModule(indexSettings, parentInjector.getInstance(IndicesAnalysisService.class))).createChildInjector(parentInjector);

        return injector.getInstance(AnalysisService.class);
    }

    public static SimilarityLookupService newSimilarityLookupService() {
        return new SimilarityLookupService(new Index("test"), ImmutableSettings.Builder.EMPTY_SETTINGS);
    }
}
