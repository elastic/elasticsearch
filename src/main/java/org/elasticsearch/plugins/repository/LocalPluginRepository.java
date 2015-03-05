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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.PluginsService;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;
import java.util.Iterator;

import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;

/**
 * Local plugin repository used to manage installed plugins.
 */
public class LocalPluginRepository implements PluginRepository {

    public static final ImmutableSet<String> FORBIDDEN_NAMES = ImmutableSet.of("elasticsearch", "elasticsearch.bat", "elasticsearch.in.sh",
                                                                                "plugin", "plugin.bat", "service.bat");

    private final static String BIN = "bin";
    private final static String CONFIG = "config";
    private final static String SITE = "_site";
    private final static String JAR = ".jar";
    private final static String CLASS = ".class";
    private final static String JAVA = ".java";

    private final String name;

    private final Path pluginsDir;
    private final Path binDir;
    private final Path configDir;

    private final Settings settings;

    private LocalPluginRepository(String name, Environment env, Settings settings) {
        this.name = name;

        this.binDir = env.homeFile().resolve(BIN);
        this.configDir = env.configFile();
        this.pluginsDir = env.pluginsFile();

        this.settings = settings;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Collection<PluginDescriptor> find(final String name) {
        return list(new Filter() {
            @Override
            public boolean accept(PluginDescriptor plugin) {
                return StringUtils.equalsIgnoreCase(name, plugin.name());
            }
        });
    }

    @Override
    public PluginDescriptor find(final String organisation, final String name, final String version) {
        // List plugins with same organisation (if known), name and version
        Collection<PluginDescriptor> plugins = list(new Filter() {
            @Override
            public boolean accept(PluginDescriptor plugin) {
                boolean match = true;
                if ((organisation != null) && (plugin.organisation() != null)) {
                    match = StringUtils.equalsIgnoreCase(organisation, plugin.organisation());
                }
                return match
                        && StringUtils.equalsIgnoreCase(name, plugin.name())
                        && StringUtils.equalsIgnoreCase(version, plugin.version());
            }
        });

        // The same plugin can't exist multiple times... if so, reports the error
        // because a manual clean up will be necessary
        if ((plugins != null) && (!plugins.isEmpty())) {
            if (plugins.size() > 1) {
                throw new PluginRepositoryException(name(), "Plugin [organisation=" + organisation
                        + ", name=" + name + ", version=" + version + "] exists in multiple instances");
            }

            return plugins.iterator().next();
        }
        return null;
    }

    @Override
    public Collection<PluginDescriptor> list() {
        return list(new Filter() {
            @Override
            public boolean accept(PluginDescriptor plugin) {
                // Accepts all plugins
                return true;
            }
        });
    }

    @Override
    public Collection<PluginDescriptor> list(final Filter filter) {
        // Get the es-plugin.properties filename
        String esPluginProps = PluginsService.ES_PLUGIN_PROPERTIES;
        if (settings != null) {
            esPluginProps = settings.get(PluginsService.ES_PLUGIN_PROPERTIES_FILE_KEY, PluginsService.ES_PLUGIN_PROPERTIES);
        }

        final ImmutableList.Builder<PluginDescriptor> plugins = ImmutableList.builder();

        String pluginName;
        PluginDescriptor.Builder descriptor;

        if (Files.exists(pluginsDir)) {
            try (DirectoryStream<Path> root = Files.newDirectoryStream(pluginsDir)) {
                for (Path pluginDir : root) {
                    if (Files.isDirectory(pluginDir)) {

                        // Determine the name of the plugin from the directory's name
                        pluginName = pluginDir.getFileName().toString();
                        descriptor = pluginDescriptor(pluginName);

                        // Introspect JAR files to find plugin's description
                        try (DirectoryStream<Path> jarFiles = Files.newDirectoryStream(pluginDir, "*" + JAR)) {
                            for (Path jarFile : jarFiles) {
                                // Indicates that the plugin has code/library
                                descriptor.jvm();

                                String fileName = jarFile.getFileName().toString();
                                if (!fileName.contains(pluginName)) {
                                    continue;
                                }

                                // Constructs the plugin's descriptor from the JAR filename...
                                // Since plugin's JAR file can have various filenames,
                                // the result is incertain

                                // Removes "elasticsearch-" prefix
                                String s = StringUtils.removeStart(fileName, "elasticsearch-");

                                // Removes ".jar" suffix
                                s = StringUtils.removeEnd(s, JAR);

                                // Removes plugin's name (detected in preVisit() method)
                                s = StringUtils.removeStart(s, pluginName);

                                // And finally try to extract the version
                                String version = StringUtils.substringAfterLast(s, "-");

                                if (Strings.hasText(version)) {
                                    descriptor.version(version);
                                }

                                // Now, overide everthing with es-plugin.properties or meta.yml file
                                // contained in the JAR file
                                descriptor.loadFromZip(this.settings, jarFile);
                            }
                        }

                        Path site = pluginDir.resolve(SITE);
                        if (Files.isDirectory(site)) {
                            descriptor.site();

                            Path esPlugin = site.resolve(esPluginProps);
                            if (Files.exists(esPlugin)) {
                                descriptor.loadFromProperties(esPlugin);
                            }
                        }

                        if (descriptor != null) {
                            PluginDescriptor plugin = descriptor.build();
                            if ((filter == null) || (filter.accept(plugin))) {
                                plugins.add(plugin);
                            }
                        }
                    }
                }
            } catch (IOException e) {
                throw new PluginRepositoryException(name(), "Error when listing the local plugin repository content", e);
            }
        }
        return plugins.build();
    }

    @Override
    public void install(final PluginDescriptor plugin) {
        if (!Strings.hasText(plugin.name())) {
            throw new PluginRepositoryException(name(), "Unable to install a plugin with a null or empty name");
        }

        validatePluginName(plugin.name());

        if (plugin.artifact() == null) {
            throw new PluginRepositoryException(name(), "Unable to install a plugin with no artifact");
        }

        try (FileSystem fs = FileSystems.newFileSystem(plugin.artifact(), null)) {
            for (Path root : fs.getRootDirectories()) {

                //we check whether we need to remove the top-level folder while installing
                //sometimes (e.g. github) the downloaded archive contains a top-level
                // folder which needs to be removed
                Path[] files = FileSystemUtils.files(root);


                // Check the valid names if the zip has only one top level directory
                boolean skipFirstLevel = false;
                if ((files != null) && (files.length == 1)) {
                    Path top = files[0];

                    if (Files.isDirectory(top)) {
                        String name = top.getFileName().toString();

                        if (!"_site/".equals(name) && !"bin/".equals(name) && !"config/".equals(name) && !"_dict/".equals(name)) {
                            skipFirstLevel = true;
                        }
                    }
                }

                final int skipLevels = (skipFirstLevel ? 1 : 0);

                // Walk the file tree and installs the plugin's files
                Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        installFile(plugin.name(), file, skipLevels);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        installFile(plugin.name(), dir, skipLevels);
                        return FileVisitResult.CONTINUE;
                    }
                });

                Path pluginDir = pluginsDir.resolve(plugin.name());
                Path siteDir = pluginDir.resolve(SITE);

                // Moves everything in 'plugins/<plugin dir>/*' under 'plugins/<plugin dir>/_site/*'
                // iff the plugin directory does not contain .class or .jar file.
                if (!Files.exists(siteDir) && !FileSystemUtils.hasExtensions(pluginDir, CLASS, JAR)) {
                    // No .class or .jar files in 'plugins/<plugin dir>/*', we assume it's a site plugin
                    try (DirectoryStream<Path> stream = Files.newDirectoryStream(pluginDir)) {
                        Iterator<Path> it = stream.iterator();
                        if (it.hasNext()) {
                            Files.createDirectories(siteDir);
                        }

                        while (it.hasNext()) {
                            Path siteFile = it.next();
                            FileSystemUtils.move(siteFile, siteDir.resolve(siteFile.getFileName()));
                        }
                    }
                }
            }
        } catch (Exception e) {

            // Remove the partially installed plugin
            remove(plugin);

            throw new PluginRepositoryException(name(), "Error when installing the plugin '" + plugin.name() + "': " + e.getMessage(), e);
        }
    }

    /**
     * Install a source path in a destination path.
     *
     * @param name
     * @param source
     * @param skippedLevels
     * @return
     * @throws IOException
     */
    private Path installFile(String name, Path source, int skippedLevels) throws IOException {
        if ((source.getFileName() != null) && (source.getFileName().toString().endsWith(JAVA))) {
            throw new PluginRepositoryException(name(), "Plugin contains source code, aborting installation");
        }

        boolean override = true;
        boolean executable = false;

        // Finds the destination of the file
        Path destination = pluginsDir.resolve(name);
        int depth = 0;

        for (Path sub : source.toAbsolutePath()) {
            // We search for 'bin/' or 'config/' in the 2 first path levels,
            // if found we skip those directories
            if (depth < (skippedLevels + 2)) {
                if (sub.startsWith(BIN)) {
                    destination = binDir.resolve(name);
                    executable = true;
                    continue;
                } else if (sub.startsWith(CONFIG)) {
                    destination = configDir.resolve(name);
                    override = false;
                    continue;
                }
            }

            if (depth++ < skippedLevels) {
                continue;
            }
            destination = destination.resolve(sub.toString());
        }

        if (Files.isDirectory(source)) {
            try {
                Files.createDirectories(destination);
            } catch (FileAlreadyExistsException e) {
                // Directory already exist, it's OK to ignore
            }
        } else {
            // Do not override the file
            if ((!override) && (Files.exists(destination))) {
                if (FileSystemUtils.isSameFile(source, destination)) {
                    // File is unchanged, do nothing
                    return destination;
                }
                // We just append a suffix to the file
                destination = destination.getParent().resolve(destination.toString().concat(".new"));
            }

            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);

            // Makes the file executable
            if ((executable) && (Files.getFileStore(destination).supportsFileAttributeView(PosixFileAttributeView.class))) {
                if (Files.isRegularFile(destination)) {
                    Files.setPosixFilePermissions(destination, Sets.newHashSet(PosixFilePermission.OWNER_EXECUTE,
                            PosixFilePermission.GROUP_EXECUTE,
                            PosixFilePermission.OTHERS_EXECUTE));
                }
            }
        }

        return destination;
    }

    @Override
    public void remove(PluginDescriptor plugin) {
        if (!Strings.hasText(plugin.name())) {
            throw new PluginRepositoryException(name(), "Unable to install a plugin with a null or empty name");
        }

        validatePluginName(plugin.name());

        try {
            // Remove the plugin's files in 'bin/' and 'plugins/' directories
            for (Path dir : Lists.newArrayList(binDir, pluginsDir)) {
                Path pluginDir = dir.resolve(plugin.name());

                if (Files.exists(pluginDir)) {
                    try {
                        IOUtils.rm(pluginDir);
                    } catch (IOException ex) {
                        throw new IOException("Unable to remove directory " + pluginDir + ", please check permissions.", ex);
                    }
                }
            }
        } catch (Exception e) {
            throw new PluginRepositoryException(name(), "Error when removing the plugin '" + plugin.name() + "': " + e.getMessage(), e);
        }
    }

    private void validatePluginName(String pluginName) {
        for (String name : LocalPluginRepository.FORBIDDEN_NAMES) {
            if (name.equalsIgnoreCase(pluginName)) {
                throw new PluginRepositoryException(name(), "Plugin's name cannot be equal to '" + pluginName + "'");
            }
        }
    }

    public static class Builder {

        private String name;
        private Environment environment;
        private Settings settings;

        public Builder(String name) {
            this.name = name;
        }

        public static Builder localRepository(String name) {
            return new Builder(name);
        }

        public Builder environment(Environment environment) {
            this.environment = environment;
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public LocalPluginRepository build() {
            return new LocalPluginRepository(name, environment, settings);
        }
    }
}
