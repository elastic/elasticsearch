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

package org.elasticsearch.cloud.azure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import java.io.IOException;

public class AzureTestUtils {
    /**
     * Read settings from file when running integration tests with ThirdParty annotation.
     * elasticsearch.yml file path has to be set with -Dtests.config=/path/to/elasticsearch.yml.
     * @return Settings from elasticsearch.yml integration test file (for 3rd party tests)
     */
    public static Settings readSettingsFromFile() {
        Settings.Builder settings = Settings.builder();

        // if explicit, just load it and don't load from env
        try {
            if (Strings.hasText(System.getProperty("tests.config"))) {
                try {
                    settings.loadFromPath(PathUtils.get((System.getProperty("tests.config"))));
                } catch (IOException e) {
                    throw new IllegalArgumentException("could not load azure tests config", e);
                }
            } else {
                throw new IllegalStateException("to run integration tests, you need to set -Dtests.thirdparty=true and " +
                    "-Dtests.config=/path/to/elasticsearch.yml");
            }
        } catch (SettingsException exception) {
            throw new IllegalStateException("your test configuration file is incorrect: " + System.getProperty("tests.config"), exception);
        }
        return settings.build();
    }
}
