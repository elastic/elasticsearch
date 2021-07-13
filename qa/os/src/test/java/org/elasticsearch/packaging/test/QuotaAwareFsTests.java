/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.After;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.elasticsearch.packaging.util.Distribution.Platform.WINDOWS;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Check that the quota-aware filesystem plugin can be installed, and that it operates as expected.
 */
public class QuotaAwareFsTests extends PackagingTestCase {

    // private static final String QUOTA_AWARE_FS_PLUGIN_NAME = "quota-aware-fs";
    private static final Path QUOTA_AWARE_FS_PLUGIN;
    static {
        // Re-read before each test so the plugin path can be manipulated within tests.
        // Corresponds to DistroTestPlugin#QUOTA_AWARE_FS_PLUGIN_SYSPROP
        QUOTA_AWARE_FS_PLUGIN = Paths.get(System.getProperty("tests.quota-aware-fs-plugin"));
    }

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only archives", distribution.isArchive());
        assumeFalse("not on windows", distribution.platform == WINDOWS);
    }

    @After
    public void teardown() throws Exception {
        super.teardown();
        cleanup();
    }

    /**
     * Check that when the plugin is installed but the system property for passing the location of the related
     * properties file is omitted, then Elasticsearch exits with the expected error message.
     */
    public void test10ElasticsearchRequiresSystemPropertyToBeSet() throws Exception {
        install();

        installation.executables().pluginTool.run("install --batch \"" + QUOTA_AWARE_FS_PLUGIN.toUri() + "\"");

        // Without setting the `es.fs.quota.file` property, ES should exit with a failure code.
        final Shell.Result result = runElasticsearchStartCommand(null, false, false);

        assertThat("Elasticsearch should have terminated unsuccessfully", result.isSuccess(), equalTo(false));
        assertThat(
            result.stderr,
            containsString("Property es.fs.quota.file must be set to a URI in order to use the quota filesystem provider")
        );
    }

    /**
     * Check that when the plugin is installed but the system property for passing the location of the related
     * properties file contains a non-existent URI, then Elasticsearch exits with the expected error message.
     */
    public void test20ElasticsearchRejectsNonExistentPropertiesLocation() throws Exception {
        install();

        installation.executables().pluginTool.run("install --batch \"" + QUOTA_AWARE_FS_PLUGIN.toUri() + "\"");

        sh.getEnv().put("ES_JAVA_OPTS", "-Des.fs.quota.file=file:///this/does/not/exist.properties");

        final Shell.Result result = runElasticsearchStartCommand(null, false, false);

        // Generate a Path for this location so that the platform-specific line-endings will be used.
        final String platformPath = Path.of("/this/does/not/exist.properties").toString();

        assertThat("Elasticsearch should have terminated unsuccessfully", result.isSuccess(), equalTo(false));
        assertThat(result.stderr, containsString("NoSuchFileException: " + platformPath));
    }

    /**
     * Check that Elasticsearch can load the plugin and apply the quota limits in the properties file. Also check that
     * Elasticsearch polls the file for changes.
     */
    public void test30ElasticsearchStartsWhenSystemPropertySet() throws Exception {
        install();

        int total = 20 * 1024 * 1024;
        int available = 10 * 1024 * 1024;

        installation.executables().pluginTool.run("install --batch \"" + QUOTA_AWARE_FS_PLUGIN.toUri() + "\"");

        final Path quotaPath = getRootTempDir().resolve("quota.properties");
        Files.writeString(quotaPath, String.format(Locale.ROOT, "total=%d\nremaining=%d\n", total, available));

        sh.getEnv().put("ES_JAVA_OPTS", "-Des.fs.quota.file=" + quotaPath.toUri());

        startElasticsearchAndThen(() -> {
            final Totals actualTotals = fetchFilesystemTotals();

            assertThat(actualTotals.totalInBytes, equalTo(total));
            assertThat(actualTotals.availableInBytes, equalTo(available));

            int updatedTotal = total * 3;
            int updatedAvailable = available * 3;

            // Check that ES is polling the properties file for changes by modifying the properties file
            // and waiting for ES to pick up the changes.
            Files.writeString(quotaPath, String.format(Locale.ROOT, "total=%d\nremaining=%d\n", updatedTotal, updatedAvailable));

            // The check interval is 1000ms, but give ourselves some leeway.
            Thread.sleep(2000);

            final Totals updatedActualTotals = fetchFilesystemTotals();

            assertThat(updatedActualTotals.totalInBytes, equalTo(updatedTotal));
            assertThat(updatedActualTotals.availableInBytes, equalTo(updatedAvailable));
        });
    }

    /**
     * Check that the _cat API can list the plugin correctly.
     */
    public void test40CatApiFiltersPlugin() throws Exception {
        install();

        int total = 20 * 1024 * 1024;
        int available = 10 * 1024 * 1024;

        installation.executables().pluginTool.run("install --batch \"" + QUOTA_AWARE_FS_PLUGIN.toUri() + "\"");

        final Path quotaPath = getRootTempDir().resolve("quota.properties");
        Files.writeString(quotaPath, String.format(Locale.ROOT, "total=%d\nremaining=%d\n", total, available));

        sh.getEnv().put("ES_JAVA_OPTS", "-Des.fs.quota.file=" + quotaPath.toUri());

        startElasticsearchAndThen(() -> {
            final String uri = "http://localhost:9200/_cat/plugins?include_bootstrap=true&h=component,type";
            String response = ServerUtils.makeRequest(Request.Get(uri)).trim();
            assertThat(response, not(emptyString()));

            List<String> lines = response.lines().collect(Collectors.toList());
            assertThat(lines, hasSize(1));

            final String[] fields = lines.get(0).split(" ");
            assertThat(fields, arrayContaining("quota-aware-fs", "bootstrap"));
        });
    }

    private void startElasticsearchAndThen(CheckedRunnable<Exception> runnable) throws Exception {
        boolean started = false;
        try {
            startElasticsearch();
            started = true;

            runnable.run();
        } finally {
            if (started) {
                stopElasticsearch();
            }
        }
    }

    private static class Totals {
        int totalInBytes;
        int availableInBytes;

        Totals(int totalInBytes, int availableInBytes) {
            this.totalInBytes = totalInBytes;
            this.availableInBytes = availableInBytes;
        }
    }

    private Totals fetchFilesystemTotals() {
        try {
            final String response = ServerUtils.makeRequest(Request.Get("http://localhost:9200/_nodes/stats"));

            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode rootNode = mapper.readTree(response);

            assertThat("Some nodes failed", rootNode.at("/_nodes/failed").intValue(), equalTo(0));

            final String nodeId = rootNode.get("nodes").fieldNames().next();

            final JsonNode fsNode = rootNode.at("/nodes/" + nodeId + "/fs/total");

            return new Totals(fsNode.get("total_in_bytes").intValue(), fsNode.get("available_in_bytes").intValue());
        } catch (Exception e) {
            throw new RuntimeException("Failed to fetch filesystem totals: " + e.getMessage(), e);
        }
    }
}
