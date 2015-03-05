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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;

public class URLPluginRepository implements PluginRepository {

    private final String name;
    private final Collection<String> urls;
    private final PluginDownloader downloader;

    protected URLPluginRepository(String name, Collection<String> urls, PluginDownloader downloader) {
        this.name = name;
        this.urls = urls;
        this.downloader = downloader;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Collection<PluginDescriptor> find(String name) {
        ImmutableList.Builder<PluginDescriptor> plugins = ImmutableList.builder();

        // It could be smarter than that (like a real search by name) but for now
        // it resolves all URLs and downloads every plugin if found.
        if (urls != null) {
            for (String pattern : urls) {
                try {
                    PluginDescriptor plugin = download(pattern, null, name, null);
                    if (plugin != null) {
                        plugins.add(plugin);
                    }
                } catch (IOException e) {
                    // The find method ignore the download exception,
                    // it should be reported by the listener.
                }
            }
         }
        return plugins.build();
    }

    @Override
    public PluginDescriptor find(String organisation, String name, String version) {
        if (urls != null) {
            for (String pattern : urls) {
                try {
                    PluginDescriptor plugin = download(pattern, organisation, name, version);
                    if (plugin != null) {
                        return plugin;
                    }
                } catch (IOException e) {
                    // The find method ignore the download exception,
                    // it should be reported by the listener.
                }
            }
        }
        return null;
    }

    private PluginDescriptor download(String pattern, String organisation, String name, String version) throws IOException {
        if (downloader == null) {
            throw new PluginRepositoryException(name(), "No downloader defined for this plugin repository");
        }

        String url = convert(pattern, organisation, name, version);
        if (url != null) {
            Path downloaded = downloader.download(url);
            if (downloaded != null) {
                PluginDescriptor plugin = pluginDescriptor(name).loadFromZip(downloaded).artifact(downloaded).build();

                // Check that the downloaded plugin corresponds to what has been asked for
                if (StringUtils.equals(name, plugin.name())) {
                    return plugin;
                } else {
                    Files.deleteIfExists(downloaded);
                }
            }
        }
        return null;
    }

    @Override
    public Collection<PluginDescriptor> list() {
        throw new UnsupportedOperationException("listing URL plugin repositories is not supported");
    }

    @Override
    public Collection<PluginDescriptor> list(Filter filter) {
        throw new UnsupportedOperationException("listing URL plugin repositories is not supported");
    }

    private String convert(String pattern, String organisation, String name, String version) throws MalformedURLException {
        if (pattern != null) {
            pattern = pattern.replace("[organisation]", organisation == null ? "" : organisation.replace('.', '/'))
                    .replace("[name]", name == null ? "" : name)
                    .replace("[version]", version == null ? "" : version)
                    .replace("[es.version]", Version.CURRENT.toString());
            return pattern;
        }
        return null;
    }

    @Override
    public void install(PluginDescriptor plugin) {
        throw new UnsupportedOperationException("Installing a plugin is not supported in a URL repository");
    }

    @Override
    public void remove(PluginDescriptor plugin) {
        throw new UnsupportedOperationException("Removing a plugin is not supported in a URL repository");
    }

    public static class Builder {

        private String name;
        private ImmutableSet.Builder<String> urls = ImmutableSet.builder();
        private Path downloadDir;
        private TimeValue timeout;
        private String userAgent = null;
        private String esVersion = null;

        private List<PluginDownloader.Listener> listeners = new ArrayList<>();

        public Builder(String name) {
            this.name = name;
        }

        public static Builder urlRepository(String name) {
            return new Builder(name);
        }

        public Builder url(String url) {
            urls.add(url);
            return this;
        }

        public Builder downloadDir(Path downloadDir) {
            this.downloadDir = downloadDir;
            return this;
        }

        public Builder timeout(TimeValue timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder userAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        public Builder elasticsearchVersion(String elasticsearchVersion) {
            this.esVersion = elasticsearchVersion;
            return this;
        }

        public Builder listener(PluginDownloader.Listener listener) {
            if (listener != null) {
                this.listeners.add(listener);
            }
            return this;
        }

        public URLPluginRepository build() {
            PluginDownloader downloader = new DefaultPluginDownloader(downloadDir, timeout, userAgent, esVersion);
            for (PluginDownloader.Listener listener : listeners) {
                downloader.addListener(listener);
            }
            return new URLPluginRepository(name, urls.build(), downloader);

        }
    }
}
