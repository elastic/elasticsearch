package org.elasticsearch.groovy.node

import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.node.Node

/**
 * @author kimchy (shay.banon)
 */
class GNode {

    final Node node;

    final GClient client;

    def GNode(Node node) {
        this.node = node;
        this.client = new GClient(node.client())
    }

    /**
     * The settings that were used to create the node.
     */
    def getSettings() {
        node.settings();
    }

    /**
     * Start the node. If the node is already started, this method is no-op.
     */
    def getStart() {
        node.start()
        this
    }

    /**
     * Stops the node. If the node is already started, this method is no-op.
     */
    def getStop() {
        node.stop()
        this
    }

    /**
     * Closes the node (and  {@link #stop} s if its running).
     */
    def getClose() {
        node.close()
        this
    }
}
