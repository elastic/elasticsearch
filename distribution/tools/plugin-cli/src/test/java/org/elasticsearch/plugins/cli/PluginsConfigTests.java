/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class PluginsConfigTests extends ESTestCase {

    /**
     * Check that an empty config object passes validation.
     */
    public void test_validate_acceptsEmptyConfig() throws PluginSyncException {
        PluginsConfig config = new PluginsConfig();
        config.validate(Set.of(), Set.of());
    }

    /**
     * Check that validation rejects a null plugin descriptor.
     */
    public void test_validate_rejectsNullDescriptor() {
        PluginsConfig config = new PluginsConfig();
        List<InstallablePlugin> descriptors = new ArrayList<>();
        descriptors.add(null);
        config.setPlugins(descriptors);
        final Exception e = expectThrows(RuntimeException.class, () -> config.validate(Set.of(), Set.of()));
        assertThat(e.getMessage(), equalTo("Cannot have null or empty IDs in [elasticsearch-plugins.yml]"));
    }

    /**
     * Check that validation rejects a null plugin descriptor.
     */
    public void test_validate_rejectsDescriptorWithNullId() {
        PluginsConfig config = new PluginsConfig();
        config.setPlugins(List.of(new InstallablePlugin()));
        final Exception e = expectThrows(RuntimeException.class, () -> config.validate(Set.of(), Set.of()));
        assertThat(e.getMessage(), equalTo("Cannot have null or empty IDs in [elasticsearch-plugins.yml]"));
    }

    /**
     * Check that validation rejects duplicate plugin IDs.
     */
    public void test_validate_rejectsDuplicatePluginId() {
        PluginsConfig config = new PluginsConfig();
        config.setPlugins(List.of(new InstallablePlugin("foo"), new InstallablePlugin("foo")));
        final Exception e = expectThrows(PluginSyncException.class, () -> config.validate(Set.of(), Set.of()));
        assertThat(e.getMessage(), equalTo("Duplicate plugin ID [foo] found in [elasticsearch-plugins.yml]"));
    }

    /**
     * Check that validation rejects unofficial plugins without a location
     */
    public void test_validate_rejectsUnofficialPluginWithoutLocation() {
        PluginsConfig config = new PluginsConfig();
        config.setPlugins(List.of(new InstallablePlugin("foo")));
        final Exception e = expectThrows(PluginSyncException.class, () -> config.validate(Set.of(), Set.of()));
        assertThat(e.getMessage(), equalTo("Must specify location for non-official plugin [foo] in [elasticsearch-plugins.yml]"));
    }

    /**
     * Check that validation rejects unofficial plugins with a blank location
     */
    public void test_validate_rejectsUnofficialPluginWithBlankLocation() {
        PluginsConfig config = new PluginsConfig();
        config.setPlugins(List.of(new InstallablePlugin("foo", "   ")));
        final Exception e = expectThrows(PluginSyncException.class, () -> config.validate(Set.of(), Set.of()));
        assertThat(e.getMessage(), equalTo("Empty location for plugin [foo]"));
    }

    /**
     * Check that validation rejects unofficial plugins with a blank location
     */
    public void test_validate_rejectsMalformedProxy() {
        List<String> examples = List.of("foo:bar:baz:8080", ":8080", "foo:", "foo:bar");

        for (String example : examples) {
            PluginsConfig config = new PluginsConfig();
            config.setProxy(example);
            Exception e = expectThrows(PluginSyncException.class, () -> config.validate(Set.of(), Set.of()));
            assertThat(e.getMessage(), equalTo("Malformed [proxy], expected [host:port] in [elasticsearch-plugins.yml]"));
        }
    }

    /**
     * Check that official plugin IDs are accepted.
     */
    public void test_validate_allowsOfficialPlugin() throws PluginSyncException {
        PluginsConfig config = new PluginsConfig();
        config.setPlugins(List.of(new InstallablePlugin("analysis-icu")));
        config.validate(Set.of("analysis-icu"), Set.of());
    }

    /**
     * Check that official plugins that have been migrated to modules are still accepted, despite
     * no longer being plugins.
     */
    public void test_validate_allowsMigratedPlugin() throws PluginSyncException {
        final List<InstallablePlugin> descriptors = Stream.of("azure", "gcs", "s3")
            .map(each -> new InstallablePlugin("repository-" + each))
            .toList();
        PluginsConfig config = new PluginsConfig();
        config.setPlugins(descriptors);

        config.validate(Set.of(), Set.of("repository-azure", "repository-gcs", "repository-s3"));
    }
}
