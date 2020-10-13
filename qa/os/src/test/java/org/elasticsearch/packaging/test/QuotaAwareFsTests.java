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

package org.elasticsearch.packaging.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.After;
import org.junit.BeforeClass;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeTrue;

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
    }

    @After
    public void teardown() throws Exception {
        super.teardown();
        cleanup();
    }

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

    public void test20ElasticsearchStartsWhenSystemPropertySet() throws Exception {
        install();

        int total = 20 * 1024 * 1024;
        int available = 10 * 1024 * 1024;

        installation.executables().pluginTool.run("install --batch \"" + QUOTA_AWARE_FS_PLUGIN.toUri() + "\"");

        final Path quotaPath = getRootTempDir().resolve("quota.properties");
        Files.writeString(quotaPath, String.format("total=%d\nremaining=%d\n", total, available));

        sh.getEnv().put("ES_JAVA_OPTS", "-Des.fs.quota.file=" + quotaPath.toUri());

        try {
            startElasticsearch();

            final String response = ServerUtils.makeRequest(Request.Get("http://localhost:9200/_nodes/stats"));

            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode rootNode = mapper.readTree(response);

            assertThat("Some nodes failed", rootNode.at("/_nodes/failed").intValue(), equalTo(0));

            final String nodeId = rootNode.get("nodes").fieldNames().next();

            final JsonNode fsNode = rootNode.at("/nodes/" + nodeId + "/fs/total");

            final int totalInBytes = fsNode.get("total_in_bytes").intValue();
            final int availableInBytes = fsNode.get("available_in_bytes").intValue();

            assertThat(totalInBytes, equalTo(total));
            assertThat(availableInBytes, equalTo(available));
        } finally {
            stopElasticsearch();
            Files.deleteIfExists(quotaPath);
        }
    }

}
