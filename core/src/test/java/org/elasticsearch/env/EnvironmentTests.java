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
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

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
                .put("path.home", createTempDir().toAbsolutePath())
                .putArray("path.data", tmpPaths()).build();
        return new Environment(build);
    }

    @Test
    public void testRepositoryResolution() throws IOException {
        Environment environment = newEnvironment();
        assertThat(environment.resolveRepoFile("/test/repos/repo1"), nullValue());
        assertThat(environment.resolveRepoFile("test/repos/repo1"), nullValue());
        environment = newEnvironment(settingsBuilder().putArray("path.repo", "/test/repos", "/another/repos", "/test/repos/../other").build());
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

}
