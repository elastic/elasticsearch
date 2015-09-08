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
import org.elasticsearch.plugin.repository.s3.S3RepositoryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ThirdParty;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for AWS tests that require credentials.
 * <p>
 * You must specify {@code -Dtests.thirdparty=true -Dtests.config=/path/to/config}
 * in order to run these tests.
 */
@ThirdParty
public abstract class AbstractAwsTestCase extends ESIntegTestCase {

    /**
     * Those properties are set by the AWS SDK v1.9.4 and if not ignored,
     * lead to tests failure (see AbstractRandomizedTest#IGNORED_INVARIANT_PROPERTIES)
     */
    private static final String[] AWS_INVARIANT_PROPERTIES = {
            "com.sun.org.apache.xml.internal.dtm.DTMManager",
            "javax.xml.parsers.DocumentBuilderFactory"
    };

    private Map<String, String> properties = new HashMap<>();

    @Before
    public void saveProperties() {
        for (String p : AWS_INVARIANT_PROPERTIES) {
            properties.put(p, System.getProperty(p));
        }
    }

    @After
    public void restoreProperties() {
        for (String p : AWS_INVARIANT_PROPERTIES) {
            if (properties.get(p) != null) {
                System.setProperty(p, properties.get(p));
            } else {
                System.clearProperty(p);
            }
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder settings = Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("path.home", createTempDir())
                .extendArray("plugin.types", S3RepositoryPlugin.class.getName(), TestAwsS3Service.TestPlugin.class.getName())
                .put("cloud.aws.test.random", randomInt())
                .put("cloud.aws.test.write_failures", 0.1)
                .put("cloud.aws.test.read_failures", 0.1);

        // if explicit, just load it and don't load from env
        try {
            if (Strings.hasText(System.getProperty("tests.config"))) {
                settings.loadFromPath(PathUtils.get(System.getProperty("tests.config")));
            } else {
                throw new IllegalStateException("to run integration tests, you need to set -Dtest.thirdparty=true and -Dtests.config=/path/to/elasticsearch.yml");
            }
        } catch (SettingsException exception) {
            throw new IllegalStateException("your test configuration file is incorrect: " + System.getProperty("tests.config"), exception);
        }
        return settings.build();
    }
}
