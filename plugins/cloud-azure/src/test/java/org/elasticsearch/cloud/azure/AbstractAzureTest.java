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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.plugin.cloud.azure.CloudAzurePlugin;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ThirdParty;

/**
 * Base class for Azure tests that require credentials.
 * <p>
 * You must specify {@code -Dtests.thirdparty=true -Dtests.config=/path/to/config}
 * in order to run these tests.
 */
@ThirdParty
public abstract class AbstractAzureTest extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", CloudAzurePlugin.class.getName())
                .put(readSettingsFromFile())
                .build();
    }

    protected Settings readSettingsFromFile() {
        Settings.Builder settings = Settings.builder();
        settings.put("path.home", createTempDir());
        Environment environment = new Environment(settings.build());

        // if explicit, just load it and don't load from env
        try {
            if (Strings.hasText(System.getProperty("tests.config"))) {
                settings.loadFromUrl(environment.resolveConfig(System.getProperty("tests.config")));
            } else {
                throw new IllegalStateException("to run integration tests, you need to set -Dtests.thirdparty=true and -Dtests.config=/path/to/elasticsearch.yml");
            }
        } catch (FailedToResolveConfigException exception) {
          throw new IllegalStateException("your test configuration file is incorrect: " + System.getProperty("tests.config"), exception);
        }
        return settings.build();
    }
}
