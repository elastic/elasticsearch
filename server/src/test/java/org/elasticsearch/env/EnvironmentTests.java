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
package org.elasticsearch.env;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

/**
 * Simple unit-tests for Environment.java
 */
public class EnvironmentTests extends ESTestCase {
    public Environment newEnvironment() throws IOException {
        return newEnvironment(Settings.EMPTY);
    }

    public Environment newEnvironment(Settings settings) throws IOException {
        Settings build = Settings.builder()
                .put(settings)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths()).build();
        return new Environment(build, null);
    }

    public void testRepositoryResolution() throws IOException {
        Environment environment = newEnvironment();
        assertThat(environment.resolveRepoFile("/test/repos/repo1"), nullValue());
        assertThat(environment.resolveRepoFile("test/repos/repo1"), nullValue());
        environment = newEnvironment(Settings.builder().putList(Environment.PATH_REPO_SETTING.getKey(), "/test/repos", "/another/repos", "/test/repos/../other").build());
        assertThat(environment.resolveRepoFile("/test/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoFile("test/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoFile("/another/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoFile("/test/repos/../repo1"), nullValue());
        assertThat(environment.resolveRepoFile("/test/repos/../repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoFile("/somethingeles/repos/repo1"), nullValue());
        assertThat(environment.resolveRepoFile("/test/other/repo"), notNullValue());


        assertThat(environment.resolveRepoURL(new URL("file:///test/repos/repo1")), notNullValue());
        assertThat(environment.resolveRepoURL(new URL("file:/test/repos/repo1")), notNullValue());
        assertThat(environment.resolveRepoURL(new URL("file://test/repos/repo1")), nullValue());
        assertThat(environment.resolveRepoURL(new URL("file:///test/repos/../repo1")), nullValue());
        assertThat(environment.resolveRepoURL(new URL("http://localhost/test/")), nullValue());

        assertThat(environment.resolveRepoURL(new URL("jar:file:///test/repos/repo1!/repo/")), notNullValue());
        assertThat(environment.resolveRepoURL(new URL("jar:file:/test/repos/repo1!/repo/")), notNullValue());
        assertThat(environment.resolveRepoURL(new URL("jar:file:///test/repos/repo1!/repo/")).toString(), endsWith("repo1!/repo/"));
        assertThat(environment.resolveRepoURL(new URL("jar:file:///test/repos/../repo1!/repo/")), nullValue());
        assertThat(environment.resolveRepoURL(new URL("jar:http://localhost/test/../repo1?blah!/repo/")), nullValue());
    }

    public void testPathDataWhenNotSet() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.dataFiles(), equalTo(new Path[]{pathHome.resolve("data")}));
    }

    public void testPathDataNotSetInEnvironmentIfNotSet() {
        final Settings settings = Settings.builder().put("path.home", createTempDir().toAbsolutePath()).build();
        assertFalse(Environment.PATH_DATA_SETTING.exists(settings));
        final Environment environment = new Environment(settings, null);
        assertFalse(Environment.PATH_DATA_SETTING.exists(environment.settings()));
    }

    public void testPathLogsWhenNotSet() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.logsFile(), equalTo(pathHome.resolve("logs")));
    }

    public void testDefaultConfigPath() {
        final Path path = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", path).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.configFile(), equalTo(path.resolve("config")));
    }

    public void testConfigPath() {
        final Path configPath = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", createTempDir().toAbsolutePath()).build();
        final Environment environment = new Environment(settings, configPath);
        assertThat(environment.configFile(), equalTo(configPath));
    }

    public void testConfigPathWhenNotSet() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.configFile(), equalTo(pathHome.resolve("config")));
    }

    public void testNodeDoesNotRequireLocalStorage() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings =
                Settings.builder()
                        .put("path.home", pathHome)
                        .put("node.local_storage", false)
                        .put("node.master", false)
                        .put("node.data", false)
                        .build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.dataFiles(), arrayWithSize(0));
    }

    public void testNodeDoesNotRequireLocalStorageButHasPathData() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Path pathData = pathHome.resolve("data");
        final Settings settings =
                Settings.builder()
                        .put("path.home", pathHome)
                        .put("path.data", pathData)
                        .put("node.local_storage", false)
                        .put("node.master", false)
                        .put("node.data", false)
                        .build();
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> new Environment(settings, null));
        assertThat(e, hasToString(containsString("node does not require local storage yet path.data is set to [" + pathData + "]")));
    }

}
