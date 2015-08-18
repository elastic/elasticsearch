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

package org.elasticsearch.index.analysis;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNameModule;
import org.elasticsearch.index.settings.IndexSettingsModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

import java.nio.file.Path;

public class AnalysisTestsHelper {

    public static AnalysisService createAnalysisServiceFromClassPath(Path baseDir, String resource) {
        Settings settings = Settings.settingsBuilder()
                .loadFromStream(resource, AnalysisTestsHelper.class.getResourceAsStream(resource))
                .put("path.home", baseDir.toString())
                .build();

        return createAnalysisServiceFromSettings(settings);
    }

    public static AnalysisService createAnalysisServiceFromSettings(
            Settings settings) {
        Index index = new Index("test");
        if (settings.get(IndexMetaData.SETTING_VERSION_CREATED) == null) {
            settings = Settings.builder().put(settings).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        }
        IndicesModule indicesModule = new IndicesModule(settings) {
            @Override
            public void configure() {
                // skip services
                bindHunspellExtension();
            }
        };
        Injector parentInjector = new ModulesBuilder().add(new SettingsModule(settings),
                new EnvironmentModule(new Environment(settings)), indicesModule).createInjector();

        AnalysisModule analysisModule = new AnalysisModule(settings,
                parentInjector.getInstance(IndicesAnalysisService.class));

        Injector injector = new ModulesBuilder().add(new IndexSettingsModule(index, settings),
                new IndexNameModule(index), analysisModule).createChildInjector(parentInjector);

        return injector.getInstance(AnalysisService.class);
    }
}
