/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.tribe;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.license.plugin.LicensePlugin;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * This class tests different scenarios around tribe node configuration, to make sure that we properly validate
 * tribes settings depending on how they will load shield or not. Main goal is to make sure that all tribes will run
 * shield too if the tribe node does.
 */
public class TribeShieldLoadedTests extends ElasticsearchTestCase {

    @Test
    public void testShieldLoadedOnBothTribeNodeAndClients() {
        //all good if the plugin is loaded on both tribe node and tribe clients, no matter how it gets loaded (manually or from classpath)
        ImmutableSettings.Builder builder = defaultSettings();
        if (randomBoolean()) {
            builder.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                    .put("plugin.types", ShieldPlugin.class.getName() + "," + LicensePlugin.class.getName());
        }
        if (randomBoolean()) {
            builder.put("tribe.t1.plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                    .put("tribe.t1.plugin.types", ShieldPlugin.class.getName() + "," + LicensePlugin.class.getName());
        }

        Node node = null;
        try {
            node = NodeBuilder.nodeBuilder().settings(builder.build()).build();
            node.start();
        } finally {
            stop(node);
        }
    }

    //this test causes leaking threads to be left behind
    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch/issues/9107")
    @Test
    public void testShieldLoadedOnTribeNodeOnly() {
        //startup failure if any of the tribe clients doesn't have shield installed
        ImmutableSettings.Builder builder = defaultSettings();
        if (randomBoolean()) {
            builder.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                    .put("plugin.types", ShieldPlugin.class.getName() + "," + LicensePlugin.class.getName());
        }

        builder.put("tribe.t1.plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false);

        try {
            NodeBuilder.nodeBuilder().settings(builder.build()).build();
            fail("node initialization should have failed due to missing shield plugin");
        } catch(Throwable t) {
            assertThat(t.getMessage(), containsString("Missing mandatory plugins [shield]"));
        }
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch/issues/9107")
    @Test
    public void testShieldMustBeLoadedOnAllTribes() {
        //startup failure if any of the tribe clients doesn't have shield installed
        ImmutableSettings.Builder builder = addTribeSettings(defaultSettings(), "t2");
        if (randomBoolean()) {
            builder.put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                    .put("plugin.types", ShieldPlugin.class.getName() + "," + LicensePlugin.class.getName());
        }

        //load shield explicitly on tribe t1
        builder.put("tribe.t1.plugin.types", ShieldPlugin.class.getName() + "," + LicensePlugin.class.getName())
                //disable loading from classpath on tribe t2 only
                .put("tribe.t2.plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false);

        try {
            NodeBuilder.nodeBuilder().settings(builder.build()).build();
            fail("node initialization should have failed due to missing shield plugin");
        } catch(Throwable t) {
            assertThat(t.getMessage(), containsString("Missing mandatory plugins [shield]"));
        }
    }

    private static void stop(Node node) {
        if (node != null) {
            try {
                node.stop();
            } catch(Throwable t) {
                //ignore
            } finally {
                node.close();
            }
        }
    }

    private static ImmutableSettings.Builder defaultSettings() {
        return addTribeSettings(ImmutableSettings.builder().put("node.name", "tribe_node"), "t1");
    }

    private static ImmutableSettings.Builder addTribeSettings(ImmutableSettings.Builder settingsBuilder, String tribe) {
        String tribePrefix = "tribe." + tribe + ".";
        return settingsBuilder.put(tribePrefix + "cluster.name", "non_existing_cluster")
                .put(tribePrefix + "discovery.type", "local")
                .put(tribePrefix + "discovery.initial_state_timeout", 0);
    }
}
