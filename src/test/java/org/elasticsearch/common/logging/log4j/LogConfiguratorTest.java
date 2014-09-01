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


package org.elasticsearch.common.logging.log4j;

import com.google.common.io.Files;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.core.Is.is;

public class LogConfiguratorTest extends ElasticsearchTestCase {


    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testResolveConfigValidFilename() throws Exception {
        File tempFileYml = File.createTempFile("logging.", ".yml", temporaryFolder.getRoot());
        File tempFileYaml = File.createTempFile("logging.", ".yaml", temporaryFolder.getRoot());

        Files.write("yml: bar", tempFileYml, StandardCharsets.UTF_8);
        Files.write("yaml: bar", tempFileYaml, StandardCharsets.UTF_8);
        Environment environment = new Environment(
                ImmutableSettings.builder().put("path.conf", temporaryFolder.getRoot().getAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), is("bar"));
        assertThat(logSettings.get("yaml"), is("bar"));
    }

    @Test
    public void testResolveConfigInvalidFilename() throws Exception {
        File tempFile = File.createTempFile("logging.", ".yml.bak", temporaryFolder.getRoot());

        Files.write("yml: bar", tempFile, StandardCharsets.UTF_8);
        Environment environment = new Environment(
                ImmutableSettings.builder().put("path.conf", temporaryFolder.getRoot().getAbsolutePath()).build());

        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        LogConfigurator.resolveConfig(environment, builder);

        Settings logSettings = builder.build();
        assertThat(logSettings.get("yml"), Matchers.nullValue());
    }
}