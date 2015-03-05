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

import com.google.common.collect.Lists;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolTestCase;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.repository.PluginDescriptor;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, transportClientRatio = 0.0)
public class PluginManagerIntegrationTests extends AbstractPluginsTests {

    private Path home = null;

    private Settings testPluginSettings() {
        assertNotNull(home);
        return ImmutableSettings.settingsBuilder()
                .put(Node.HTTP_ENABLED, true)
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .put("path.home", home.toAbsolutePath())
                .build();
    }

    @Before
    public void setUpHome() {
        home = newTempDirPath();
    }

    /**
     * Main method to test a plugin with the PluginManagerIntegrationTests.
     * <p/>
     * It installs the plugin with the PluginManager, starts a node and  checks with the NodesInfo API is the plugin is loaded.
     *
     * @param fqn   the fully qualified name of the plugin
     * @param zip   the zip file of the plugin to test
     * @param files the expected files of the plugin
     * @param env   the environment
     */
    @Override
    protected void testPlugin(String fqn, Path zip, Collection<String> files, Environment env) throws Exception {
        logger.info("-> Testing plugin '{}'", fqn);
        assertNotNull(fqn);

        // Creates the plugin descriptor corresponding to the plugin to test
        final PluginDescriptor plugin = pluginDescriptor(fqn).build();
        assertNotNull(plugin);

        List<String> args = Lists.newArrayList(fqn);
        if (zip != null) {
            args.add("--url");
            args.add(zip.toUri().toString());
        }

        Settings settings = testPluginSettings();
        Environment environment = new Environment(settings);

        // Installs the plugin
        CliToolTestCase.CaptureOutputTerminal terminal = new CliToolTestCase.CaptureOutputTerminal();
        CliTool.ExitStatus status = new PluginManager(terminal)
                .parse("install", args.toArray(new String[]{}))
                .execute(settings, environment);

        assertThat(status, equalTo(CliTool.ExitStatus.OK));
        assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Installing plugin [" + plugin.name() + "]...")));
        assertThat(terminal.getTerminalOutput(), hasItem(startsWith("Plugin installed successfully!")));

        // Starts the cluster
        internalCluster().startNode(settings);

        // If the files contains a '_site' folder  we can check if the plugin is correctly loaded
        final AtomicBoolean checkSite = new AtomicBoolean(false);

        Files.walkFileTree(environment.pluginsFile(), new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                String dirname = dir.getFileName().toString();
                if ("_site".equals(dirname)) {
                    checkSite.set(true);
                }
                return FileVisitResult.CONTINUE;
            }
        });

        if (checkSite.get()) {
            // Checks if the HTTP transport is started
            boolean httpStarted = awaitBusy(new com.google.common.base.Predicate<Object>() {

                @Override
                public boolean apply(Object input) {
                    try {
                        HttpResponse response = newHttpRequestBuilder().method("GET").path("/").execute();
                        if (response.getStatusCode() != RestStatus.OK.getStatus()) {
                            // We want to trace what's going on here before failing the test
                            logger.info("--> error caught [{}], headers [{}]", response.getStatusCode(), response.getHeaders());
                            logger.info("--> cluster state [{}]", internalCluster().clusterService().state());
                            return false;
                        }
                        return true;
                    } catch (IOException e) {
                        throw new ElasticsearchException("HTTP problem", e);
                    }
                }
            }, 5, TimeUnit.SECONDS);
            assertThat("HTTP transport must be started before checking for plugin _site", httpStarted, equalTo(true));

            // Checks if the plugin _site is available
            HttpResponse response = newHttpRequestBuilder().method("GET").path("/_plugin/" + plugin.name() + "/").execute();
            assertThat(response, notNullValue());
            assertThat(response.getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        }
    }

    private HttpRequestBuilder newHttpRequestBuilder() {
        return new HttpRequestBuilder(HttpClients.createDefault()).httpTransport(internalCluster().getInstance(HttpServerTransport.class));
    }
}