package org.elasticsearch.groovy.node

import org.elasticsearch.groovy.util.json.JsonBuilder
import org.elasticsearch.node.Node
import org.elasticsearch.node.internal.InternalNode
import org.elasticsearch.util.settings.ImmutableSettings
import org.elasticsearch.util.settings.loader.JsonSettingsLoader

/**
 * @author kimchy (shay.banon)
 */
public class GNodeBuilder {

    private final ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();

    private boolean loadConfigSettings = true;

    public static GNodeBuilder nodeBuilder() {
        new GNodeBuilder()
    }

    def settings(Closure settings) {
        byte[] settingsBytes = new JsonBuilder().buildAsBytes(settings);
        settingsBuilder.put(new JsonSettingsLoader().load(settingsBytes))
    }

    def getBuild() {
        Node node = new InternalNode(settingsBuilder.build(), loadConfigSettings)
        new GNode(node)
    }

    def getNode() {
        build.start
    }
}
