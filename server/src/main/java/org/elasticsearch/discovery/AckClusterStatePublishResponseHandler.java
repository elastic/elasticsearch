/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.discovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Set;

/**
 * Allows to wait for all nodes to reply to the publish of a new cluster state
 * and notifies the {@link org.elasticsearch.discovery.Discovery.AckListener}
 * so that the cluster state update can be acknowledged
 */
public class AckClusterStatePublishResponseHandler extends BlockingClusterStatePublishResponseHandler {

    private static final Logger logger = LogManager.getLogger(AckClusterStatePublishResponseHandler.class);

    private final Discovery.AckListener ackListener;

    /**
     * Creates a new AckClusterStatePublishResponseHandler
     * @param publishingToNodes the set of nodes to which the cluster state will be published and should respond
     * @param ackListener the {@link org.elasticsearch.discovery.Discovery.AckListener} to notify for each response
     *                    gotten from non master nodes
     */
    public AckClusterStatePublishResponseHandler(Set<DiscoveryNode> publishingToNodes, Discovery.AckListener ackListener) {
        // Don't count the master as acknowledged, because it's not done yet
        // otherwise we might end up with all the nodes but the master holding the latest cluster state
        super(publishingToNodes);
        this.ackListener = ackListener;
    }

    @Override
    public void onResponse(DiscoveryNode node) {
        super.onResponse(node);
        onNodeAck(ackListener, node, null);
    }

    @Override
    public void onFailure(DiscoveryNode node, Exception e) {
        try {
            super.onFailure(node, e);
        } finally {
            onNodeAck(ackListener, node, e);
        }
    }

    private void onNodeAck(final Discovery.AckListener ackListener, DiscoveryNode node, Exception e) {
        try {
            ackListener.onNodeAck(node, e);
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.debug(() -> new ParameterizedMessage("error while processing ack for node [{}]", node), inner);
        }
    }
}
