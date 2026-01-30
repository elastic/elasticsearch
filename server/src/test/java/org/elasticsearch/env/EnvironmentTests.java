/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.env;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Simple unit-tests for Environment.java
 */
public class EnvironmentTests extends ESTestCase {

    public void testRepositoryResolution() throws IOException {
        Environment environment = newEnvironment();
        assertThat(environment.resolveRepoDir("/test/repos/repo1"), nullValue());
        assertThat(environment.resolveRepoDir("test/repos/repo1"), nullValue());
        environment = newEnvironment(
            Settings.builder()
                .putList(Environment.PATH_REPO_SETTING.getKey(), "/test/repos", "/another/repos", "/test/repos/../other")
                .build()
        );
        assertThat(environment.resolveRepoDir("/test/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoDir("test/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoDir("/another/repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoDir("/test/repos/../repo1"), nullValue());
        assertThat(environment.resolveRepoDir("/test/repos/../repos/repo1"), notNullValue());
        assertThat(environment.resolveRepoDir("/somethingeles/repos/repo1"), nullValue());
        assertThat(environment.resolveRepoDir("/test/other/repo"), notNullValue());

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
        assertThat(environment.dataDirs(), equalTo(new Path[] { pathHome.resolve("data") }));
    }

    public void testPathDataNotSetInEnvironmentIfNotSet() {
        final Settings settings = Settings.builder().put("path.home", createTempDir().toAbsolutePath()).build();
        assertFalse(Environment.PATH_DATA_SETTING.exists(settings));
        final Environment environment = new Environment(settings, null);
        assertFalse(Environment.PATH_DATA_SETTING.exists(environment.settings()));
    }

    public void testPathDataLegacyCommaList() {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir().toAbsolutePath())
            .put("path.data", createTempDir().toAbsolutePath() + "," + createTempDir().toAbsolutePath())
            .build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.dataDirs(), arrayWithSize(2));
    }

    public void testPathLogsWhenNotSet() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.logsDir(), equalTo(pathHome.resolve("logs")));
    }

    public void testDefaultConfigPath() {
        final Path path = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", path).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.configDir(), equalTo(path.resolve("config")));
    }

    public void testConfigPath() {
        final Path configPath = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", createTempDir().toAbsolutePath()).build();
        final Environment environment = new Environment(settings, configPath);
        assertThat(environment.configDir(), equalTo(configPath));
    }

    public void testConfigPathWhenNotSet() {
        final Path pathHome = createTempDir().toAbsolutePath();
        final Settings settings = Settings.builder().put("path.home", pathHome).build();
        final Environment environment = new Environment(settings, null);
        assertThat(environment.configDir(), equalTo(pathHome.resolve("config")));
    }

    public void testNonExistentTempPathValidation() {
        Settings build = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = new Environment(build, null, createTempDir().resolve("this_does_not_exist"));
        FileNotFoundException e = expectThrows(FileNotFoundException.class, environment::validateTmpDir);
        assertThat(e.getMessage(), startsWith("Temporary directory ["));
        assertThat(e.getMessage(), endsWith("this_does_not_exist] does not exist or is not accessible"));
    }

    public void testTempPathValidationWhenRegularFile() throws IOException {
        Settings build = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = new Environment(build, null, createTempFile("something", ".test"));
        IOException e = expectThrows(IOException.class, environment::validateTmpDir);
        assertThat(e.getMessage(), startsWith("Temporary directory ["));
        assertThat(e.getMessage(), endsWith(".test] is not a directory"));
    }

    public void testNonExistentTempPathValidationForNatives() {
        Settings build = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = new Environment(build, null, createTempDir().resolve("this_does_not_exist"));
        FileNotFoundException e = expectThrows(FileNotFoundException.class, environment::validateNativesConfig);
        assertThat(e.getMessage(), startsWith("Temporary directory ["));
        assertThat(e.getMessage(), endsWith("this_does_not_exist] does not exist or is not accessible"));
    }

    public void testTempPathValidationWhenRegularFileForNatives() throws IOException {
        Settings build = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        Environment environment = new Environment(build, null, createTempFile("something", ".test"));
        IOException e = expectThrows(IOException.class, environment::validateNativesConfig);
        assertThat(e.getMessage(), startsWith("Temporary directory ["));
        assertThat(e.getMessage(), endsWith(".test] is not a directory"));
    }

    // test that environment paths are absolute and normalized
    public void testPathNormalization() throws IOException {
        final Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), "home")
            .put(Environment.PATH_DATA_SETTING.getKey(), "./home/../home/data")
            .put(Environment.PATH_LOGS_SETTING.getKey(), "./home/../home/logs")
            .put(Environment.PATH_REPO_SETTING.getKey(), "./home/../home/repo")
            .put(Environment.PATH_SHARED_DATA_SETTING.getKey(), "./home/../home/shared_data")
            .build();

        // the above paths will be treated as relative to the working directory
        final Path workingDirectory = PathUtils.get(System.getProperty("user.dir"));

        final Environment environment = new Environment(settings, null, createTempDir());
        final String homePath = Environment.PATH_HOME_SETTING.get(environment.settings());
        assertPath(homePath, workingDirectory.resolve("home"));

        final Path home = PathUtils.get(homePath);

        final List<String> dataPaths = Environment.PATH_DATA_SETTING.get(environment.settings());
        assertThat(dataPaths, hasSize(1));
        assertPath(dataPaths.get(0), home.resolve("data"));

        final String logPath = Environment.PATH_LOGS_SETTING.get(environment.settings());
        assertPath(logPath, home.resolve("logs"));

        final List<String> repoPaths = Environment.PATH_REPO_SETTING.get(environment.settings());
        assertThat(repoPaths, hasSize(1));
        assertPath(repoPaths.get(0), home.resolve("repo"));

        final String sharedDataPath = Environment.PATH_SHARED_DATA_SETTING.get(environment.settings());
        assertPath(sharedDataPath, home.resolve("shared_data"));
    }

    public void testSingleDataPathListCheck() {
        {
            final Settings settings = Settings.builder().build();
            assertThat(Environment.dataPathUsesList(settings), is(false));
        }
        {
            final Settings settings = Settings.builder()
                .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString(), createTempDir().toString())
                .build();
            assertThat(Environment.dataPathUsesList(settings), is(true));
        }
        {
            final Settings settings = Settings.builder()
                .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString())
                .build();
            assertThat(Environment.dataPathUsesList(settings), is(true));
        }
        {
            final Settings settings = Settings.builder()
                .put(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toString() + "," + createTempDir().toString())
                .build();
            assertThat(Environment.dataPathUsesList(settings), is(true));
        }
    }

    private void assertPath(final String actual, final Path expected) {
        assertIsAbsolute(actual);
        assertIsNormalized(actual);
        assertThat(PathUtils.get(actual), equalTo(expected));
    }

    private void assertIsAbsolute(final String path) {
        assertTrue("path [" + path + "] is not absolute", PathUtils.get(path).isAbsolute());
    }

    private void assertIsNormalized(final String path) {
        assertThat("path [" + path + "] is not normalized", PathUtils.get(path), equalTo(PathUtils.get(path).normalize()));
    }
}
