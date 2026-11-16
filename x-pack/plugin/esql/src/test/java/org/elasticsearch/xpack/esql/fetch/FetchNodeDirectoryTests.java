/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * How a coordinator finds the nodes that hold the fetch contexts of its queries.
 */
public class FetchNodeDirectoryTests extends ESTestCase {
    private static final DiscoveryNode LOCAL_NODE = DiscoveryNodeUtils.create("local-1");

    private final FetchNodeDirectory directory = new FetchNodeDirectory(() -> DiscoveryNodes.builder().add(LOCAL_NODE).build());

    public void testFindsANodeOfTheLocalClusterInItsClusterState() {
        assertThat(find(FetchNode.local("local-1")).actionGet(), sameInstance(LOCAL_NODE));
    }

    public void testANodeThatLeftTheLocalClusterIsNotFound() {
        NoNodeAvailableException e = expectThrows(NoNodeAvailableException.class, find(FetchNode.local("local-2"))::actionGet);
        assertThat(e.getMessage(), equalTo("node [local-2] left the cluster"));
    }

    /**
     * The fetch phase reads the local cluster only, so a node of a remote cluster is a bug, even with the id of a local
     * node.
     */
    public void testANodeOfARemoteClusterIsRefused() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, find(new FetchNode("remote", "local-1"))::actionGet);
        assertThat(e.getMessage(), containsString("is a node of a remote cluster"));
    }

    private PlainActionFuture<DiscoveryNode> find(FetchNode node) {
        PlainActionFuture<DiscoveryNode> found = new PlainActionFuture<>();
        directory.find(node, found);
        return found;
    }
}
