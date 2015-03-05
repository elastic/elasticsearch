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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class PluginDescriptor {

    public static final String META_FILE = "meta.yml";
    private static final String FQN_SEPARATOR = "/";

    private final String organisation;
    private final String name;
    private final String version;
    private final String description;

    private final Set<PluginDescriptor> dependencies;

    private final Path artifact;

    private PluginDescriptor(String organisation, String name, String version, String description, Set<PluginDescriptor> dependencies, Path artifact) {
        this.organisation = organisation;
        this.name = name;
        this.version = version;
        this.description = description;
        this.dependencies = dependencies;
        this.artifact = artifact;
    }

    public String organisation() {
        return organisation;
    }

    public String name() {
        return name;
    }

    public String version() {
        return version;
    }

    public String description() {
        return description;
    }

    public Set<PluginDescriptor> dependencies() {
        return dependencies;
    }

    public Path artifact() {
        return artifact;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginDescriptor that = (PluginDescriptor) o;

        if (!name.equals(that.name)) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return organisation + FQN_SEPARATOR + name + FQN_SEPARATOR + version;
    }

    public static class Builder {

        private String organisation;
        private String name;
        private String version;
        private String description;

        private Path artifact;

        private boolean site = false;
        private boolean jvm = false;

        private ImmutableSet.Builder<PluginDescriptor> dependencies = ImmutableSet.builder();

        private Builder() {
        }

        /**
         * Instantiates a plugin descriptor builder from a fully qualified name:
         * - "plugin"
         * - "organisation/plugin"
         * - "organisation/plugin/version"
         *
         * @param fqn the fully qualified name
         * @return the plugin descriptor builder
         */
        public static Builder pluginDescriptor(String fqn) {
            Builder builder = new Builder();
            builder.name(fqn);

            if (Strings.hasText(fqn)) {
                String[] elements = Strings.delimitedListToStringArray(fqn, FQN_SEPARATOR);

                // We first consider the simplest form: plugin_name
                builder.name(elements[0]);

                // We consider the form: organisation/plugin_name
                if (elements.length > 1) {
                    builder.organisation(elements[0]);
                    builder.name(elements[1]);

                    // We consider the form: organisation/plugin_name/version
                    if (elements.length > 2) {
                        builder.version(elements[2]);
                    }
                }
            }
            return builder;
        }

        public Builder organisation(String organisation) {
            this.organisation = organisation;
            return this;
        }

        public Builder name(String name) {
            this.name = StringUtils.removeStart(name, "elasticsearch-");
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder artifact(Path artifact) {
            this.artifact = artifact;
            return this;
        }

        public Builder site() {
            this.site = true;
            return this;
        }

        public Builder jvm() {
            this.jvm = true;
            return this;
        }

        public Builder dependency(String fqn) {
            dependencies.add(pluginDescriptor(fqn).build());
            return this;
        }

        public Builder dependency(PluginDescriptor dependency) {
            dependencies.add(dependency);
            return this;
        }

        public Builder dependency(String organisation, String name, String version) {
            PluginDescriptor dependency = pluginDescriptor(name).organisation(organisation).version(version).build();
            dependencies.add(dependency);
            return this;
        }

        public Builder loadFromProperties(InputStream is) throws IOException {
            Properties props = new Properties();
            props.load(is);

            String version = props.getProperty("version");
            if (Strings.hasText(version)) {
                version(version);
            }

            String desc = props.getProperty("description");
            if (Strings.hasText(desc)) {
                description(desc);
            }
            return this;
        }

        public Builder loadFromProperties(Path esPlugin) {
            try (InputStream is = Files.newInputStream(esPlugin)) {
                return loadFromProperties(is);
            } catch (IOException e) {
                throw new ElasticsearchException("Error when loading plugin descriptor from properties file [" + esPlugin.toAbsolutePath() + "]", e);
            }
        }

        public Builder loadFromMeta(Settings settings) {
            String org = settings.get("organisation");
            if (Strings.hasText(org)) {
                organisation(org);
            }
            String name = settings.get("name");
            if (Strings.hasText(name)) {
                name(name);
            }
            String version = settings.get("version");
            if (Strings.hasText(version)) {
                version(version);
            }
            String description = settings.get("description");
            if (Strings.hasText(description)) {
                description(description);
            }

            ImmutableMap<String, String> deps = settings.getByPrefix("dependencies.").getAsMap();
            if ((deps != null) && (!deps.isEmpty())) {
                int i = 0;
                String depName;

                do {
                    depName = deps.get(String.valueOf(i) + ".name");
                    String depOrganisation = deps.get(String.valueOf(i) + ".organisation");
                    String depVersion = deps.get(String.valueOf(i) + ".version");
                    if (Strings.hasText(depName)) {
                        dependency(depOrganisation, depName, depVersion);
                    }
                    i++;
                } while ((depName != null) && (i < 1000));
            }
            return this;
        }

        public Builder loadFromMeta(InputStream is) {
            return loadFromMeta(ImmutableSettings.builder().loadFromStream(META_FILE, is).build());
        }

        public Builder loadFromZip(Path zip) {
            return loadFromZip(ImmutableSettings.EMPTY, zip);
        }

        public Builder loadFromZip(Settings settings, Path zip) {

            // Get the es-plugin.properties filename
            String esPluginProps = PluginsService.ES_PLUGIN_PROPERTIES;
            if (settings != null) {
                esPluginProps = settings.get(PluginsService.ES_PLUGIN_PROPERTIES_FILE_KEY, PluginsService.ES_PLUGIN_PROPERTIES);
            }

            try (FileSystem zipfs = FileSystems.newFileSystem(zip, null)) {
                for (final Path root : zipfs.getRootDirectories()) {
                    try (DirectoryStream<Path> files = Files.newDirectoryStream(root)) {
                        for (Path file : files) {
                            String name = file.getFileName().toString();

                            if (StringUtils.equals(name, esPluginProps)) {
                                try (InputStream is = Files.newInputStream(file)) {
                                    loadFromProperties(is);
                                }
                            }

                            if (StringUtils.equals(name, "_site/")) {
                                Path props = file.resolve(esPluginProps);
                                if (Files.exists(props)) {
                                    try (InputStream is = Files.newInputStream(props)) {
                                        loadFromProperties(is);
                                    }
                                }
                            }

                            if (StringUtils.equals(name, PluginDescriptor.META_FILE)) {
                                try (InputStream is = Files.newInputStream(file)) {
                                    loadFromMeta(is);
                                }
                            }

                            if (StringUtils.endsWith(file.getFileName().toString(), ".jar")) {
                                // Read the content of the JAR file using a ZipInputStream
                                try (ZipInputStream stream = new ZipInputStream(Files.newInputStream(file))) {
                                    ZipEntry entry;
                                    while ((entry = stream.getNextEntry()) != null) {
                                        try {
                                            if (entry.isDirectory()) {
                                                continue;
                                            }

                                            // Note that the following methods must not close the current stream,
                                            // otherwise it's impossible to get the next file included in the JAR.

                                            String entryName = entry.getName();
                                            if (StringUtils.equals(entryName, esPluginProps)) {
                                                loadFromProperties(stream);
                                            }

                                            if (StringUtils.equals(entryName, PluginDescriptor.META_FILE)) {
                                                loadFromMeta(new ByteArrayInputStream(ByteStreams.toByteArray(stream)));
                                            }
                                        } finally {
                                            stream.closeEntry();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Error when loading plugin descriptor from file [" + zip.toAbsolutePath() + "]", e);
            }
            return this;
        }

        public PluginDescriptor build() {
            return new PluginDescriptor(organisation, name, version, description, dependencies.build(), artifact);
        }
    }
}
