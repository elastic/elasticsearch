package org.elasticsearch.examples.nativescript.script;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 */
public class AbstractSearchScriptTests {
    protected Node node;

    @BeforeMethod
    public void startNode() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .build();

        node = NodeBuilder.nodeBuilder().settings(settings).node();
    }

    @AfterMethod
    public void closeNode() {
        if (node != null) {
            node.close();
        }
    }
}
