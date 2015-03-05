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

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.AbstractPluginsTests;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collection;

import static org.elasticsearch.plugins.AbstractPluginsTests.ZippedFileBuilder.createZippedFile;
import static org.elasticsearch.plugins.repository.LocalPluginRepository.Builder.localRepository;
import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Unit test class for {@link org.elasticsearch.plugins.repository.LocalPluginRepository}
 */
public class LocalPluginRepositoryTests extends AbstractPluginsTests {

    /**
     * Main method to test a plugin with the LocalPluginRepository.
     * <p/>
     * It basically installs the plugin, checks the files, then uses the find() and list() methods
     * to check that the plugin is correctly installed and found, then removes the plugin and finally
     * checks that files are deleted.
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
        assertNotNull(zip);

        // Creates the plugin descriptor corresponding to the plugin to test
        final PluginDescriptor plugin = pluginDescriptor(fqn).artifact(zip).build();
        assertNotNull(plugin);

        // Creates the local repository
        LocalPluginRepository repository = localRepository("unit-test").environment(env).build();

        // Installs the plugin in the local repository
        repository.install(plugin);

        // Print all installed files
        if (logger.isDebugEnabled()) {
            logger.debug("-> List of installed files:");
            Files.walkFileTree(env.homeFile(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    logger.debug("dir : {}", dir);
                    return super.preVisitDirectory(dir, attrs);
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    logger.debug("file: {}", file);
                    return super.visitFile(file, attrs);
                }
            });
        }

        // Checks that files have been copied
        if (files != null) {
            for (String file : files) {
                Path f = env.homeFile().resolve(file);
                assertTrue("File must exist: " + f, Files.exists(f));

                if (file.startsWith("bin/" + plugin.name())) {
                    // check that the file is marked executable, without actually checking that we can execute it.
                    PosixFileAttributeView view = Files.getFileAttributeView(f, PosixFileAttributeView.class);
                    // the view might be null, on e.g. windows, there is nothing to check there!
                    if (view != null) {
                        PosixFileAttributes attributes = view.readAttributes();
                        assertTrue("unexpected permissions: " + attributes.permissions(),
                                attributes.permissions().contains(PosixFilePermission.OWNER_EXECUTE));
                    }
                }
            }
        }

        // Finds all plugins with corresponding name
        Collection<PluginDescriptor> matchs = repository.find(plugin.name());
        assertNotNull(matchs);
        assertThat(matchs.size(), equalTo(1));
        assertThat(matchs, hasItem(plugin));

        // Finds all plugins with corresponding name
        matchs = repository.find(plugin.name());
        assertNotNull(matchs);
        assertThat(matchs.size(), equalTo(1));
        assertThat(matchs, hasItem(plugin));

        // Finds plugin with corresponding organisation/name/version
        PluginDescriptor match = repository.find(plugin.organisation(), plugin.name(), plugin.version());
        assertNotNull(match);
        assertThat(match, equalTo(plugin));

        // List all plugins
        matchs = repository.list();
        assertNotNull(matchs);
        assertThat(matchs.size(), equalTo(1));
        assertThat(matchs, hasItem(plugin));

        // List all with same name
        matchs = repository.list(new PluginRepository.Filter() {
            @Override
            public boolean accept(PluginDescriptor p) {
                return StringUtils.equals(p.name(), plugin.name());
            }
        });
        assertNotNull(matchs);
        assertThat(matchs.size(), equalTo(1));
        assertThat(matchs, hasItem(plugin));

        // List all with same version
        matchs = repository.list(new PluginRepository.Filter() {
            @Override
            public boolean accept(PluginDescriptor p) {
                return StringUtils.equals(p.version(), plugin.version());
            }
        });
        assertNotNull(matchs);
        assertThat(matchs.size(), equalTo(1));
        assertThat(matchs, hasItem(plugin));

        // Removes the plugin
        repository.remove(plugin);

        // Check that files are correctly deleted
        if (files != null) {
            for (String file : files) {
                // Config files are never removed
                if (file.startsWith("config")) {
                    assertTrue("Files must exist: " + file, Files.exists(env.homeFile().resolve(file)));
                } else {
                    assertFalse("File must not exist: " + file, Files.exists(env.homeFile().resolve(file)));
                }
            }
        }

        // Now the plugin is not found
        matchs = repository.find(plugin.name());
        assertNotNull(matchs);
        assertThat(matchs.size(), equalTo(0));

        matchs = repository.list();
        assertNotNull(matchs);
        assertThat(matchs.size(), equalTo(0));

        logger.info("-> Plugin '{}' tested with success", plugin.name());
    }

    /**
     * Test to plugins with a forbidden names
     */
    @Test
    public void testPluginWithForbiddenNames() throws Exception {
        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            // Creates the local repository
            LocalPluginRepository repository = localRepository("unit-test").environment(env).build();

            for (String name : LocalPluginRepository.FORBIDDEN_NAMES) {
                PluginDescriptor plugin = pluginDescriptor(name).build();
                assertNotNull(plugin);

                try {
                    repository.install(plugin);
                    fail("should have thrown a PluginRepositoryException about installing a plugin with a forbidden name");
                } catch (PluginRepositoryException e) {
                    assertThat(e.getMessage().contains("Plugin's name cannot be equal to"), Matchers.equalTo(true));
                }

                try {
                    repository.remove(plugin);
                    fail("should have thrown a PluginRepositoryException about removing a plugin with a forbidden name");
                } catch (PluginRepositoryException e) {
                    assertThat(e.getMessage().contains("Plugin's name cannot be equal to"), Matchers.equalTo(true));
                }
            }
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- /plugin-with-sourcefiles/
     *       +-- src/main/java/org/elasticsearch/plugin/foo/bar/FooBar.java
     *       +-- bin/
     *            +-- cmd.sh
     */
    @Test
    public void testPluginWithSourceFilesAndConfig() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_with_sourcefiles_and_bin.zip")
                .addDir("plugin-with-sourcefiles-and-bin/bin/")
                .addFile("plugin-with-sourcefiles-and-bin/bin/cmd.sh", "Command file")
                .addDir("plugin-with-sourcefiles-and-bin/src/main/java/org/elasticsearch/plugin/foo/bar/")
                .addFile("plugin-with-sourcefiles-and-bin/src/main/java/org/elasticsearch/plugin/foo/bar/FooBar.java", "package foo.bar")
                .toPath();

        InMemoryEnvironment env = null;
        try {
            env = new InMemoryEnvironment();
            testPlugin("plugin-with-sourcefiles-and-bin", zip, null, env);
            fail("should have thrown an exception about the existence of .java files");

        } catch (PluginRepositoryException e) {
            assertThat("message contains error about the existence of .java file: " + e.getMessage(),
                    e.getMessage().contains("Plugin contains source code, aborting installation"), Matchers.equalTo(true));

            // Checks that the plugin has been removed
            assertNotNull(env);
            assertFalse(Files.exists(env.pluginsFile().resolve("plugin-with-sourcefiles-and-bin")));
            assertFalse(Files.exists(env.homeFile().resolve("bin").resolve("plugin-with-sourcefiles-and-bin")));
        } finally {
            if (env != null) {
                env.close();
            }
        }
    }

    /**
     * Test a plugin with the following zipped content :
     * <p/>
     * |
     * +-- /plugin-with-sourcefiles/src/main/java/org/elasticsearch/plugin/foo/bar/FooBar.java
     */
    @Test
    public void testPluginWithSourceFiles() throws Exception {
        Path zip = createZippedFile(newTempDirPath(), "plugin_with_sourcefiles.zip")
                .addDir("plugin-with-sourcefiles/src/main/java/org/elasticsearch/plugin/foo/bar/")
                .addFile("plugin-with-sourcefiles/src/main/java/org/elasticsearch/plugin/foo/bar/FooBar.java", "package foo.bar")
                .toPath();

        try (InMemoryEnvironment env = new InMemoryEnvironment()) {
            testPlugin("plugin-with-sourcefiles", zip, null, env);
            fail("should have thrown an exception about the existence of .java files");

        } catch (PluginRepositoryException e) {
            assertThat("message contains error about the existence of .java file: " + e.getMessage(),
                    e.getMessage().contains("Plugin contains source code, aborting installation"), Matchers.equalTo(true));
        }

    }
}
