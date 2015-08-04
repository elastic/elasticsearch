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
package org.elasticsearch.plugins;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;

import java.nio.file.Path;

import static org.apache.lucene.util.Constants.WINDOWS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasStatus;

@ClusterScope(scope = SUITE, numDataNodes = 1)
public class SitePluginRelativePathConfigIT extends ESIntegTestCase {

    private final Path root = PathUtils.get(".").toAbsolutePath().getRoot();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String cwdToRoot = getRelativePath(PathUtils.get(".").toAbsolutePath());
        Path pluginDir = PathUtils.get(cwdToRoot, relativizeToRootIfNecessary(getDataPath("/org/elasticsearch/test_plugins")).toString());

        Path tempDir = createTempDir();
        boolean useRelativeInMiddleOfPath = randomBoolean();
        if (useRelativeInMiddleOfPath) {
            pluginDir = PathUtils.get(tempDir.toString(), getRelativePath(tempDir), pluginDir.toString());
        }

        return settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("path.plugins", pluginDir)
                .put("force.http.enabled", true)
                .build();
    }

    @Test
    public void testThatRelativePathsDontAffectPlugins() throws Exception {
        HttpResponse response = httpClient().method("GET").path("/_plugin/dummy/").execute();
        assertThat(response, hasStatus(OK));
    }

    private Path relativizeToRootIfNecessary(Path path) {
        if (WINDOWS) {
            return root.relativize(path);
        }
        return path;
    }

    private String getRelativePath(Path path) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < path.getNameCount(); i++) {
            sb.append("..");
            sb.append(path.getFileSystem().getSeparator());
        }

        return sb.toString();
    }

    public HttpRequestBuilder httpClient() {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        return new HttpRequestBuilder(httpClient).httpTransport(internalCluster().getDataNodeInstance(HttpServerTransport.class));
    }
}
