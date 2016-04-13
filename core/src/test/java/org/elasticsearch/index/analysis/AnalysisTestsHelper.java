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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.HunspellService;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;

public class AnalysisTestsHelper {

    public static AnalysisService createAnalysisServiceFromClassPath(Path baseDir, String resource) throws IOException {
        Settings settings = Settings.builder()
                .loadFromStream(resource, AnalysisTestsHelper.class.getResourceAsStream(resource))
                .put(Environment.PATH_HOME_SETTING.getKey(), baseDir.toString())
                .build();

        return createAnalysisServiceFromSettings(settings);
    }

    public static AnalysisService createAnalysisServiceFromSettings(
            Settings settings) throws IOException {
        if (settings.get(IndexMetaData.SETTING_VERSION_CREATED) == null) {
            settings = Settings.builder().put(settings).put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).build();
        }
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings("test", settings);
        Environment environment = new Environment(settings);
        return new AnalysisRegistry(new HunspellService(settings, environment, Collections.emptyMap()), environment).build(idxSettings);
    }
}
