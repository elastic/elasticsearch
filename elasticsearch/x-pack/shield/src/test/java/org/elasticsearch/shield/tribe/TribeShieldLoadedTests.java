/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.tribe;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackPlugin;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.containsString;

/**
 * This class tests different scenarios around tribe node configuration, to make sure that we properly validate
 * tribes settings depending on how they will load shield or not. Main goal is to make sure that all tribes will run
 * shield too if the tribe node does.
 */
public class TribeShieldLoadedTests extends ESTestCase {
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/13212")
    // we really need to support loading plugins at the node level this way which should flow the plugins down to the tribe service, right now it doesnt!
    public void testShieldLoadedOnBothTribeNodeAndClients() {
        //all good if the plugin is loaded on both tribe node and tribe clients, no matter how it gets loaded (manually or from classpath)
        Settings.Builder builder = defaultSettings();

        try (Node node = new MockNode(builder.build(), Version.CURRENT, Arrays.asList(XPackPlugin.class, XPackPlugin.class))) {
            node.start();
        }
    }

    //this test causes leaking threads to be left behind
    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch/issues/9107")
    public void testShieldLoadedOnTribeNodeOnly() {
        //startup failure if any of the tribe clients doesn't have shield installed
        Settings.Builder builder = defaultSettings();

        try {
            new Node(builder.build());
            fail("node initialization should have failed due to missing shield plugin");
        } catch(Throwable t) {
            assertThat(t.getMessage(), containsString("Missing mandatory plugins [shield]"));
        }
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch/issues/9107")
    public void testShieldMustBeLoadedOnAllTribes() {
        //startup failure if any of the tribe clients doesn't have shield installed
        Settings.Builder builder = addTribeSettings(defaultSettings(), "t2");

        try {
            new Node(builder.build());
            fail("node initialization should have failed due to missing shield plugin");
        } catch(Throwable t) {
            assertThat(t.getMessage(), containsString("Missing mandatory plugins [shield]"));
        }
    }

    private static Settings.Builder defaultSettings() {
        return addTribeSettings(Settings.builder()
                .put("node.name", "tribe_node")
                .put("path.home", createTempDir())
                .putArray("plugin.mandatory", XPackPlugin.NAME), "t1");
    }

    private static Settings.Builder addTribeSettings(Settings.Builder settingsBuilder, String tribe) {
        String tribePrefix = "tribe." + tribe + ".";
        return settingsBuilder.put(tribePrefix + "cluster.name", "non_existing_cluster")
                .put(tribePrefix + "discovery.type", "local")
                .put(tribePrefix + "discovery.initial_state_timeout", 0)
                .putArray(tribePrefix + "plugin.mandatory", XPackPlugin.NAME);
    }
}
