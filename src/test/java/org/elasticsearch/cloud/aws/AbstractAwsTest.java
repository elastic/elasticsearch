/*
 * Licensed to Elasticsearch (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 */
public abstract class AbstractAwsTest extends ElasticsearchIntegrationTest {

    /**
     * Annotation for tests that require AWS to run. AWS tests are disabled by default.
     * Look at README file for details on how to run tests
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = SYSPROP_AWS)
    public @interface AwsTest {
    }

    /**
     */
    public static final String SYSPROP_AWS = "tests.aws";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
                ImmutableSettings.Builder settings = ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .put(AwsModule.S3_SERVICE_TYPE_KEY, TestAwsS3Service.class)
                .put("cloud.aws.test.random", randomInt())
                .put("cloud.aws.test.write_failures", 0.1);

        Environment environment = new Environment();

        // if explicit, just load it and don't load from env
        try {
            if (Strings.hasText(System.getProperty("tests.config"))) {
                settings.loadFromUrl(environment.resolveConfig(System.getProperty("tests.config")));
            } else {
                fail("to run integration tests, you need to set -Dtest.aws=true and -Dtests.config=/path/to/elasticsearch.yml");
            }
        } catch (FailedToResolveConfigException exception) {
            fail("your test configuration file is incorrect: " + System.getProperty("tests.config"));
        }
        return settings.build();
    }
}
