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
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
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
     * <p/>
     * To enable test add -Dtests.aws=true -Des.config=/path/to/elasticsearch.yml
     * <p/>
     * The elasticsearch.yml file should contain the following keys
     * <pre>
     * cloud:
     *      aws:
     *          access_key: AKVAIQBF2RECL7FJWGJQ
     *          secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br
     *          region: "us-west"
     *
     * repositories:
     *      s3:
     *          bucket: "bucket_name"
     *
     * </pre>
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
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .put( AwsModule.S3_SERVICE_TYPE_KEY, TestAwsS3Service.class)
                .put("cloud.aws.test.random", randomInt())
                .put("cloud.aws.test.write_failures", 0.1)
                .build();
    }
}
