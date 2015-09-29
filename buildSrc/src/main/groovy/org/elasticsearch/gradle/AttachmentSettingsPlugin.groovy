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

package org.elasticsearch.gradle

import org.gradle.api.Plugin
import org.gradle.api.initialization.Settings

/**
 * TODO: docs
 */
class AttachmentSettingsPlugin implements Plugin<Settings> {
    @Override
    void apply(Settings settings) {
        File localFile = new File(settings.settingsDir, '.local-elasticsearch.path')
        if (localFile.exists()) {
            // already validated directory exists in buildSrc
            File path = new File(localFile.getText('UTF-8').trim())
            settings.include('elasticsearch')
            settings.project(':elasticsearch').projectDir = path
            settings.include('elasticsearch:rest-api-spec')
            settings.include('elasticsearch:core')
            settings.include('elasticsearch:test-framework')
            settings.include('elasticsearch:distribution')
            // set project name back, since including elasticsearch overwrote it
            settings.rootProject.name = 'x-plugins'
        }
    }
}
