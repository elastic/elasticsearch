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

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.Sets;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.*;

/**
 * Unit test class for {@link org.elasticsearch.plugins.repository.PluginResolver}
 */
public class PluginResolverTests extends ElasticsearchTestCase {

    private static PluginDescriptor PLUGIN_A_V1 = pluginDescriptor("elasticsearch/plugin-a/1.0.0").build();
    private static PluginDescriptor PLUGIN_A_V2 = pluginDescriptor("elasticsearch/plugin-a/2.0.0").build();
    private static PluginDescriptor PLUGIN_B_V1 = pluginDescriptor("elasticsearch/plugin-b/1.0.0").build();
    private static PluginDescriptor PLUGIN_B_V2 = pluginDescriptor("elasticsearch/plugin-b/2.0.0").build();
    private static PluginDescriptor PLUGIN_C_V1 = pluginDescriptor("elasticsearch/plugin-c/1.0.0").build();

    private static PluginDescriptor PLUGIN_X_WITH_DEP_V1 = pluginDescriptor("elasticsearch/plugin-x/1.0.0")
                                                                        .dependency(PLUGIN_A_V1)
                                                                        .build();

    private static PluginDescriptor PLUGIN_Y_WITH_DEP_V1 = pluginDescriptor("elasticsearch/plugin-y/1.0.0")
                                                                        .dependency(PLUGIN_B_V1)
                                                                        .dependency(PLUGIN_C_V1)
                                                                        .dependency(PLUGIN_X_WITH_DEP_V1)
                                                                        .build();

    private static PluginDescriptor PLUGIN_Z_WITH_CIRCULAR_DEP_V1 = pluginDescriptor("elasticsearch/plugin-z/1.0.0")
                                                                        .dependency(PLUGIN_A_V1)
                                                                        .dependency(PLUGIN_B_V1)
                                                                        .dependency("elasticsearch/plugin-z-prime/1.0.0")
                                                                        .build();

    private static PluginDescriptor PLUGIN_Z_PRIME_V1 = pluginDescriptor("elasticsearch/plugin-z-prime/1.0.0")
                                                                        .dependency(PLUGIN_B_V1)
                                                                        .dependency(PLUGIN_C_V1)
                                                                        .dependency("elasticsearch/plugin-z-second/1.0.0")
                                                                        .build();

    private static PluginDescriptor PLUGIN_Z_SECOND_V1 = pluginDescriptor("elasticsearch/plugin-z-second/1.0.0")
                                                                        // Circular reference
                                                                        .dependency("elasticsearch/plugin-z/1.0.0")
                                                                        .build();

    /**
     * Resolve a plugin when nothing is installed locally.
     */
    @Test
    public void testSimpleResolutionWhenNoPluginsInstalled() {

        // Local:
        //  - empty
        PluginRepository local = new TestPluginRepository();

        // Remote:
        //  - elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_A_V1);

        // The plugin 'plugin-a' must be found
        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_A_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(1));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin when it is already installed.
     */
    @Test
    public void testExistingResolution() {

        // Local:
        //  - empty
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V1);

        // Remote:
        //  - elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_A_V1);

        // The plugin 'plugin-a' is already installed
        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_A_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), is(empty()));
        assertThat(resolution.existings(), hasSize(1));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin when nothing is installed locally, but the plugin is missing.
     */
    @Test
    public void testMissingResolutionWhenNoPluginsInstalled() {

        // Local:
        //  - empty
        PluginRepository local = new TestPluginRepository();

        // Remote:
        //  - elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_A_V1);

        // The plugin 'plugin-b' must not be found
        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_B_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), is(empty()));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), hasSize(1));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin but a previous version is already installed.
     */
    @Test
    public void testConflictingResolutionWhenNoPluginsInstalled() {

        // Local:
        //  - elasticsearch/plugin-a/1.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V1);

        // Remote:
        //  - elasticsearch/plugin-a/2.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_A_V2);

        // The plugin 'plugin-a/2.0.0' is conflicting
        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_A_V2);

        assertNotNull(resolution);
        assertThat(resolution.additions(), is(empty()));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), hasSize(1));
        assertThat(resolution.conflicts(), containsInAnyOrder(new Tuple<>(PLUGIN_A_V1, PLUGIN_A_V2)));
    }

    /**
     * Resolve a plugin when other plugins are installed locally.
     */
    @Test
    public void testSimpleResolutionWhenPluginsInstalled() {

        // Local:
        //  - elasticsearch/plugin-a/1.0.0
        //  - elasticsearch/plugin-b/1.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V1);
        local.install(PLUGIN_B_V1);

        // Remote:
        //  - elasticsearch/plugin-c/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_C_V1);

        // The plugin 'plugin-c' must be found
        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_C_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(1));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin when it's already installed with other plugins.
     */
    @Test
    public void testExistingResolutionWhenPluginsInstalled() {

        // Local:
        //  - elasticsearch/plugin-a/1.0.0
        //  - elasticsearch/plugin-b/1.0.0
        //  - elasticsearch/plugin-c/1.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V1);
        local.install(PLUGIN_B_V1);
        local.install(PLUGIN_C_V1);

        // Remote:
        //  - elasticsearch/plugin-c/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_C_V1);

        // The plugin 'plugin-c' already exist
        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_C_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), is(empty()));
        assertThat(resolution.existings(), hasSize(1));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin when it's already installed with other plugins.
     */
    @Test
    public void testMissingResolutionWhenPluginsInstalled() {

        // Local:
        //  - elasticsearch/plugin-a/1.0.0
        //  - elasticsearch/plugin-b/1.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V1);
        local.install(PLUGIN_B_V1);

        // Remote:
        //  - elasticsearch/plugin-c/1.0.0
        PluginRepository remote = new TestPluginRepository();

        // The plugin 'plugin-c' must not be found
        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_C_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), is(empty()));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), hasSize(1));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin but a previous version is already installed with other plugins.
     */
    @Test
    public void testConflictingResolutionWhenPluginsInstalled() {

        // Local:
        //  - elasticsearch/plugin-a/1.0.0
        //  - elasticsearch/plugin-b/1.0.0
        //  - elasticsearch/plugin-c/1.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V1);
        local.install(PLUGIN_B_V1);
        local.install(PLUGIN_C_V1);

        // Remote:
        //  - elasticsearch/plugin-a/2.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_A_V2);

        // The plugin 'plugin-a/2.0.0' already exist in a previous version
        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_A_V2);

        assertNotNull(resolution);
        assertThat(resolution.additions(), is(empty()));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), hasSize(1));
    }

    /**
     * Resolve a plugin with 1 dependency when nothing is installed locally.
     */
    @Test
    public void testResolutionWithDependenciesWhenNoDependencyInstalled() {

        // Local:
        //  - empty
        PluginRepository local = new TestPluginRepository();

        // Remote:
        //  - elasticsearch/plugin-x/1.0.0 depends on elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_X_WITH_DEP_V1);
        remote.install(PLUGIN_A_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_X_WITH_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(2));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin with 1 dependency when the dependency is installed locally.
     */
    @Test
    public void testResolutionWithDependenciesWhenDependencyIsInstalled() {

        // Local:
        //  - empty
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V1);

        // Remote:
        //  - elasticsearch/plugin-x/1.0.0 depends on elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_X_WITH_DEP_V1);
        remote.install(PLUGIN_A_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_X_WITH_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(1));
        assertThat(resolution.existings(), hasSize(1));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin with 1 dependency when the dependency is missing.
     */
    @Test
    public void testResolutionWithDependenciesWhenDependencyIsMissing() {

        // Local:
        //  - empty
        PluginRepository local = new TestPluginRepository();

        // Remote:
        //  - elasticsearch/plugin-x/1.0.0 depends on elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_X_WITH_DEP_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_X_WITH_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(1));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), hasSize(1));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin with 1 dependency when the dependency is conflicting.
     */
    @Test
    public void testResolutionWithDependenciesWhenDependencyIsConflicting() {

        // Local:
        //  - elasticsearch/plugin-a/2.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V2);

        // Remote:
        //  - elasticsearch/plugin-x/1.0.0 depends on elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_X_WITH_DEP_V1);
        remote.install(PLUGIN_A_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_X_WITH_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(1));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), hasSize(1));
    }

    /**
     * Resolve a plugin with multiple+transitive dependencies when nothing is installed locally.
     */
    @Test
    public void testResolutionWithTransDependenciesWhenNoDependencyInstalled() {

        // Local:
        //  - empty
        PluginRepository local = new TestPluginRepository();

        // Remote:
        //  - elasticsearch/plugin-y/1.0.0 depends on :
        //              elasticsearch/plugin-b/1.0.0
        //              elasticsearch/plugin-c/1.0.0
        //              elasticsearch/plugin-x/1.0.0 depends on: elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_Y_WITH_DEP_V1);
        remote.install(PLUGIN_B_V1);
        remote.install(PLUGIN_C_V1);
        remote.install(PLUGIN_X_WITH_DEP_V1);
        remote.install(PLUGIN_A_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_Y_WITH_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(5));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin with multiple+transitive dependencies when dependency is installed locally.
     */
    @Test
    public void testResolutionWithTransDependenciesWhenDependencyIsInstalled() {

        // Local:
        //  - elasticsearch/plugin-a/1.0.0
        //  - elasticsearch/plugin-c/1.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V1);
        local.install(PLUGIN_C_V1);

        // Remote:
        //  - elasticsearch/plugin-y/1.0.0 depends on :
        //              elasticsearch/plugin-b/1.0.0
        //              elasticsearch/plugin-c/1.0.0
        //              elasticsearch/plugin-x/1.0.0 depends on: elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_Y_WITH_DEP_V1);
        remote.install(PLUGIN_B_V1);
        remote.install(PLUGIN_X_WITH_DEP_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_Y_WITH_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(3));
        assertThat(resolution.additions(), containsInAnyOrder(PLUGIN_Y_WITH_DEP_V1, PLUGIN_X_WITH_DEP_V1, PLUGIN_B_V1));

        assertThat(resolution.existings(), hasSize(2));
        assertThat(resolution.existings(), containsInAnyOrder(PLUGIN_A_V1, PLUGIN_C_V1));

        assertThat(resolution.missings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin with multiple+transitive dependencies but some dependencies are missing.
     */
    @Test
    public void testResolutionWithTransDependenciesWhenDependencyMissing() {

        // Local:
        //  - elasticsearch/plugin-b/2.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_B_V1);

        // Remote:
        //  - elasticsearch/plugin-y/1.0.0 depends on :
        //              elasticsearch/plugin-b/1.0.0
        //              elasticsearch/plugin-c/1.0.0
        //              elasticsearch/plugin-x/1.0.0 depends on: elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_Y_WITH_DEP_V1);
        remote.install(PLUGIN_B_V1);
        remote.install(PLUGIN_X_WITH_DEP_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_Y_WITH_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(2));
        assertThat(resolution.additions(), containsInAnyOrder(PLUGIN_Y_WITH_DEP_V1, PLUGIN_X_WITH_DEP_V1));

        assertThat(resolution.existings(), hasSize(1));
        assertThat(resolution.existings(), containsInAnyOrder(PLUGIN_B_V1));

        assertThat(resolution.missings(), hasSize(2));
        assertThat(resolution.missings(), containsInAnyOrder(PLUGIN_A_V1, PLUGIN_C_V1));

        assertThat(resolution.conflicts(), is(empty()));
    }

    /**
     * Resolve a plugin with multiple+transitive dependencies but some dependencies are conflicting.
     */
    @Test
    public void testResolutionWithTransDependenciesWhenDependencyIsConflicting() {

        // Local:
        //  - elasticsearch/plugin-a/2.0.0
        //  - elasticsearch/plugin-b/2.0.0
        //  - elasticsearch/plugin-c/1.0.0
        PluginRepository local = new TestPluginRepository();
        local.install(PLUGIN_A_V2);
        local.install(PLUGIN_B_V2);
        local.install(PLUGIN_C_V1);

        // Remote:
        //  - elasticsearch/plugin-y/1.0.0 depends on :
        //              elasticsearch/plugin-b/1.0.0
        //              elasticsearch/plugin-c/1.0.0
        //              elasticsearch/plugin-x/1.0.0 depends on: elasticsearch/plugin-a/1.0.0
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_Y_WITH_DEP_V1);
        remote.install(PLUGIN_B_V1);
        remote.install(PLUGIN_X_WITH_DEP_V1);
        remote.install(PLUGIN_A_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_Y_WITH_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(2));
        assertThat(resolution.additions(), containsInAnyOrder(PLUGIN_Y_WITH_DEP_V1, PLUGIN_X_WITH_DEP_V1));

        assertThat(resolution.existings(), hasSize(1));
        assertThat(resolution.existings(), containsInAnyOrder(PLUGIN_C_V1));

        assertThat(resolution.missings(), is(empty()));

        assertThat(resolution.conflicts(), hasSize(2));
        assertThat(resolution.conflicts(), containsInAnyOrder(new Tuple<>(PLUGIN_A_V2, PLUGIN_A_V1), new Tuple<>(PLUGIN_B_V2, PLUGIN_B_V1)));
    }

    /**
     * Resolve a plugin with multiple+transitive+circular dependencies when nothing is installed
     */
    @Test
    public void testResolutionWithTransCirculDependenciesWhenNothingInstalled() {

        // Local:
        PluginRepository local = new TestPluginRepository();

        // Remote:
        //  - plugin-z -> plugin-z-prime -> plugin-z-second -> plugin-z (circular)
        PluginRepository remote = new TestPluginRepository();
        remote.install(PLUGIN_Z_WITH_CIRCULAR_DEP_V1);
        remote.install(PLUGIN_Z_PRIME_V1);
        remote.install(PLUGIN_Z_SECOND_V1);
        remote.install(PLUGIN_A_V1);
        remote.install(PLUGIN_B_V1);
        remote.install(PLUGIN_C_V1);

        PluginResolver.Resolution resolution = new PluginResolver(local, remote).resolve(PLUGIN_Z_WITH_CIRCULAR_DEP_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(6));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));

        resolution = new PluginResolver(local, remote).resolve(PLUGIN_Z_PRIME_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(6));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));


        resolution = new PluginResolver(local, remote).resolve(PLUGIN_Z_SECOND_V1);

        assertNotNull(resolution);
        assertThat(resolution.additions(), hasSize(6));
        assertThat(resolution.existings(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
        assertThat(resolution.conflicts(), is(empty()));
    }

    private static class TestPluginRepository implements PluginRepository {

        private final Set<PluginDescriptor> plugins = Sets.newHashSet();

        @Override
        public String name() {
            return null;
        }

        @Override
        public Collection<PluginDescriptor> find(String name) {
            Set<PluginDescriptor> result = Sets.newHashSet();

            for (PluginDescriptor plugin : ImmutableSet.copyOf(plugins)) {
                if (plugin.name().equals(name)) {
                    result.add(plugin);
                }
            }
            return result;
        }

        @Override
        public PluginDescriptor find(String organisation, String name, String version) {
            for (PluginDescriptor plugin : ImmutableSet.copyOf(plugins)) {

                boolean match = true;
                if ((organisation != null) && (plugin.organisation() != null)) {
                    match = StringUtils.equalsIgnoreCase(organisation, plugin.organisation());
                }
                if (match
                        && StringUtils.equalsIgnoreCase(name, plugin.name())
                        && StringUtils.equalsIgnoreCase(version, plugin.version())) {
                    return plugin;
                }
            }
            return null;
        }

        @Override
        public Collection<PluginDescriptor> list() {
            return ImmutableSet.copyOf(plugins);
        }

        @Override
        public Collection<PluginDescriptor> list(Filter filter) {
            Set<PluginDescriptor> result = Sets.newHashSet();

            for (PluginDescriptor plugin : ImmutableSet.copyOf(plugins)) {
                if (filter.accept(plugin)) {
                    result.add(plugin);
                }
            }
            return result;
        }

        @Override
        public void install(PluginDescriptor plugin) {
            plugins.add(plugin);
        }

        @Override
        public void remove(PluginDescriptor plugin) {
            plugins.remove(plugin);
        }
    }

}
