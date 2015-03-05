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

package org.elasticsearch.plugins.repository;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.Version;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.plugins.AbstractPluginsTests.ZippedFileBuilder.createZippedFile;
import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;
import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Unit test class for {@link org.elasticsearch.plugins.repository.URLPluginRepository}
 */
public class URLPluginRepositoryTests extends ElasticsearchTestCase {

    private static List<String> URLS = Lists.newArrayList(
            "http://www.example.org/match/on/[organisation]/[name]/[version]",
            "http://www.example.org/match/on/[organisation]/[name]",
            "http://www.example.org/match/on/[name]",
            "http://www.example.org/match/on/[version]",
            "http://www.example.org/match/on/[organisation]",
            "http://www.example.org/always/match"
    );

    private Path DOWNLOAD;

    @Before
    public void setUpDownload() throws IOException {
        DOWNLOAD = createZippedFile(newTempDirPath(), "plugin.zip")
                    .addDir("bin/")
                    .addExecutableFile("bin/init.sh", "Init script")
                    .addDir("config/")
                    .addFile("config/plugin.yml", "Configuration file")
                    .addFile("elasticsearch-awesome-plugin.jar",
                            createZippedFile()
                                .addFile("es-plugin.properties", "version=1.2.3\n")
                                .addFile("meta.yml", "organisation: elasticsearch\nname: awesome-plugin\nversion: 1.2.3\n".getBytes(Charsets.UTF_8))
                            .toByteArray()
                    ).toPath();
    }

    @Test
    public void testFind() throws IOException {
        TestPluginDownloader downloader = new TestPluginDownloader("http://www.example.org/match/on/awesome-plugin");
        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, downloader);

        // Find all plugin that match the given names, all URLS will be tested
        Collection<PluginDescriptor> plugins = repository.find("awesome-plugin");
        assertNotNull(plugins);
        assertThat(plugins.size(), equalTo(1));
        assertThat(downloader.tests().size(), equalTo(URLS.size()));
    }

    @Test
    public void testFindWithEsVersion() throws IOException {
        TestPluginDownloader downloader = new TestPluginDownloader("http://www.example.org/match/on/org/elasticsearch/awesome-plugin/latest-" + Version.CURRENT.toString());
        URLPluginRepository repository = new URLPluginRepository("unit-test", Lists.newArrayList("http://www.example.org/match/on/[organisation]/[name]/latest-[es.version]"), downloader);

        // Find all plugin that match the given names, all URLS will be tested
        PluginDescriptor plugin = repository.find("org.elasticsearch", "awesome-plugin", "1.2.3");
        assertNotNull(plugin);
        assertThat(downloader.tests().size(), equalTo(1));
    }

    @Test
    public void testFindWithOrganisationNameVersion() throws IOException {
        TestPluginDownloader downloader = new TestPluginDownloader("http://www.example.org/match/on/elasticsearch/awesome-plugin/1.2.3");
        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, downloader);

        // Find the first plugin that match the given org/name/version, URL #1 must match
        PluginDescriptor plugin = repository.find("elasticsearch", "awesome-plugin", "1.2.3");
        assertNotNull(plugin);
        assertThat(downloader.tests().size(), equalTo(1));

        downloader.clear();

        // Wrong organisation, plugin is not found and all URLS has been tested
        plugin = repository.find("my-organisation", "awesome-plugin", "1.2.3");
        assertNull(plugin);
        assertThat(downloader.tests().size(), equalTo(URLS.size()));

        downloader.clear();

        // Wrong name, plugin is not found and all URLS has been tested
        plugin = repository.find("elasticsearch", "my-awesome-plugin", "1.2.3");
        assertNull(plugin);
        assertThat(downloader.tests().size(), equalTo(URLS.size()));

        downloader.clear();

        // Wrong version, plugin is not found and all URLS has been tested
        plugin = repository.find("elasticsearch", "my-awesome-plugin", "1.2.4");
        assertNull(plugin);
        assertThat(downloader.tests().size(), equalTo(URLS.size()));
    }

    @Test
    public void testFindWithPartialOrganisationNameVersion() throws IOException {
        TestPluginDownloader downloader = new TestPluginDownloader("http://www.example.org/match/on/elasticsearch/awesome-plugin/1.2.3",
                                                                    "http://www.example.org/match/on/elasticsearch/awesome-plugin",
                                                                    "http://www.example.org/match/on/elasticsearch",
                                                                    "http://www.example.org/match/on/awesome-plugin"
        );

        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, downloader);

        PluginDescriptor plugin = repository.find("elasticsearch", "awesome-plugin", "1.2.3");
        assertNotNull(plugin);
        assertThat(downloader.tests().size(), equalTo(1));

        downloader.clear();

        // No version provided but plugin is found when trying URL #2
        plugin = repository.find("elasticsearch", "awesome-plugin", null);
        assertNotNull(plugin);
        assertThat(downloader.tests().size(), equalTo(2));

        downloader.clear();

        // Wrong organisation but plugin is found when trying URL #3 because it matches on name
        plugin = repository.find("my-organisation", "awesome-plugin", "1.2.3");
        assertNotNull(plugin);
        assertThat(downloader.tests().size(), equalTo(3));
    }

    @Test
    public void testFindNoMatch() {
        // The PluginDownloader will try all URLs but never find something that match on 'null'
        TestPluginDownloader downloader = new TestPluginDownloader(null, null);
        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, downloader);

        Collection<PluginDescriptor> plugins = repository.find("awesome-plugin");
        assertNotNull(plugins);
        assertThat(plugins.size(), equalTo(0));
        assertThat(downloader.tests().size(), equalTo(URLS.size()));
        downloader.clear();

        PluginDescriptor plugin = repository.find("elasticsearch", "awesome-plugin", "1.0.0");
        assertNull(plugin);
    }


    @Test
    public void testFindNoDownloader() {
        // There are URLs but no downloader, it should throws an exception
        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, null);

        try {
            repository.find("awesome-plugin");
            fail("should have thrown an exception because no downloader is defined");
        } catch (PluginRepositoryException e) {
            assertThat(e.getMessage().contains("No downloader defined for this plugin repository"), Matchers.equalTo(true));
        }

        try {
            repository.find("elasticsearch", "awesome-plugin", "1.0.0");
            fail("should have thrown an exception because no downloader is defined");
        } catch (PluginRepositoryException e) {
            assertThat(e.getMessage().contains("No downloader defined for this plugin repository"), Matchers.equalTo(true));
        }
    }

    @Test
    public void testFindEmptyUrls() {
        URLPluginRepository repository = new URLPluginRepository("unit-test", null, null);

        Collection<PluginDescriptor> plugins = repository.find("awesome-plugin");
        assertNotNull(plugins);
        assertThat(plugins.size(), equalTo(0));

        PluginDescriptor plugin = repository.find("elasticsearch", "awesome-plugin", "1.0.0");
        assertNull(plugin);
    }

    @Test
    public void testList() {
        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, null);
        try {
            repository.list();
            fail("should have thrown an exception because listing URL plugin repositories is not supported");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage().contains("listing URL plugin repositories is not supported"), Matchers.equalTo(true));
        }
    }

    @Test
    public void testListWithFilter() {
        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, null);
        try {
            repository.list(new PluginRepository.Filter() {
                @Override
                public boolean accept(PluginDescriptor plugin) {
                    return plugin.name() != null;
                }
            });

            fail("should have thrown an exception because listing URL plugin repositories is not supported");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage().contains("listing URL plugin repositories is not supported"), Matchers.equalTo(true));
        }
    }

    @Test
    public void testInstall() {
        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, null);
        try {
            repository.install(pluginDescriptor("elasticsearch/awesome-plugin/1.2.3").artifact(DOWNLOAD).build());
            fail("should have thrown an exception because installing a plugin is not supported in a URL repository");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage().contains("Installing a plugin is not supported in a URL repository"), Matchers.equalTo(true));
        }
    }

    @Test
    public void testRemove() {
        URLPluginRepository repository = new URLPluginRepository("unit-test", URLS, null);
        try {
            repository.remove(pluginDescriptor("elasticsearch/awesome-plugin/1.2.3").artifact(DOWNLOAD).build());
            fail("should have thrown an exception because removing a plugin is not supported in a URL repository");
        } catch (UnsupportedOperationException e) {
            assertThat(e.getMessage().contains("Removing a plugin is not supported in a URL repository"), Matchers.equalTo(true));
        }
    }

    private class TestPluginDownloader implements PluginDownloader {

        private final Set<String> matches;
        private final List<String> tests = Lists.newArrayList();

        private TestPluginDownloader(String... matches) {
            this.matches = Sets.newHashSet(matches);
        }

        @Override
        public Path download(String source) throws IOException {
            tests.add(source);
            if (matches.isEmpty() || matches.contains(source)) {
                return DOWNLOAD;
            }
            return null;
        }

        public List<String> tests() {
            return tests;
        }

        public void clear() {
            tests.clear();
        }

        @Override
        public void addListener(Listener listener) {
        }
    }
}
