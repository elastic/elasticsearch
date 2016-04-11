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

package org.elasticsearch.cloud.aws;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugin.discovery.ec2.Ec2DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ThirdParty;

import java.io.IOException;
import java.util.Collection;

/**
 * Base class for AWS tests that require credentials.
 * <p>
 * You must specify {@code -Dtests.thirdparty=true -Dtests.config=/path/to/config}
 * in order to run these tests.
 */
@ThirdParty
public abstract class AbstractAwsTestCase extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir());

        // if explicit, just load it and don't load from env
        try {
            if (Strings.hasText(System.getProperty("tests.config"))) {
                try {
                    settings.loadFromPath(PathUtils.get(System.getProperty("tests.config")));
                } catch (IOException e) {
                    throw new IllegalArgumentException("could not load aws tests config", e);
                }
            } else {
                throw new IllegalStateException("to run integration tests, you need to set -Dtests.thirdparty=true and -Dtests.config=/path/to/elasticsearch.yml");
            }
        } catch (SettingsException exception) {
            throw new IllegalStateException("your test configuration file is incorrect: " + System.getProperty("tests.config"), exception);
        }
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(Ec2DiscoveryPlugin.class);
    }
}
