/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.transport.NoNodeAvailableException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NoSuchRemoteClusterException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

/**
 * How a coordinator finds the nodes that hold the fetch contexts of its queries, in its own cluster and in remote ones.
 */
public class FetchNodeDirectoryTests extends ESTestCase {
    private static final DiscoveryNode LOCAL_NODE = DiscoveryNodeUtils.create("local-1");
    private static final DiscoveryNode REMOTE_NODE = DiscoveryNodeUtils.create("remote-1");
    private static final DiscoveryNode JOINED_NODE = DiscoveryNodeUtils.create("remote-2");
    private static final String REMOTE = "remote";

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final RemoteCollections remote = new RemoteCollections();
    private final FetchNodeDirectory directory = new FetchNodeDirectory(
        () -> DiscoveryNodes.builder().add(LOCAL_NODE).build(),
        remote,
        threadContext
    );

    public void testFindsANodeOfTheLocalClusterInItsClusterState() {
        assertThat(find(FetchNode.local("local-1")).actionGet(), sameInstance(LOCAL_NODE));
        assertThat("the local cluster needs no collection", remote.started, hasSize(0));
    }

    public void testANodeThatLeftTheLocalClusterIsNotFound() {
        NoNodeAvailableException e = expectThrows(NoNodeAvailableException.class, find(FetchNode.local("local-2"))::actionGet);
        assertThat(e.getMessage(), equalTo("node [local-2] left the cluster"));
    }

    /**
     * The nodes of a remote cluster serve every later request, so the directory collects them once.
     */
    public void testCollectsTheNodesOfARemoteClusterOnce() {
        PlainActionFuture<DiscoveryNode> first = find(new FetchNode(REMOTE, "remote-1"));
        PlainActionFuture<DiscoveryNode> second = find(new FetchNode(REMOTE, "remote-1"));
        remote.complete(0, Map.of("remote-1", REMOTE_NODE));
        PlainActionFuture<DiscoveryNode> later = find(new FetchNode(REMOTE, "remote-1"));

        assertThat(first.actionGet(), sameInstance(REMOTE_NODE));
        assertThat(second.actionGet(), sameInstance(REMOTE_NODE));
        assertThat(later.actionGet(), sameInstance(REMOTE_NODE));
        assertThat(remote.started, equalTo(List.of(REMOTE)));
    }

    /**
     * A node that joined the remote cluster after its nodes were collected is found once they are collected again.
     */
    public void testCollectsAgainForANodeThatJoined() {
        directory.prepare(REMOTE);
        remote.complete(0, Map.of("remote-1", REMOTE_NODE));

        PlainActionFuture<DiscoveryNode> joined = find(new FetchNode(REMOTE, "remote-2"));
        remote.complete(1, Map.of("remote-1", REMOTE_NODE, "remote-2", JOINED_NODE));

        assertThat(joined.actionGet(), sameInstance(JOINED_NODE));
        assertThat(remote.started, hasSize(2));
    }

    /**
     * A node that isn't among the nodes of its remote cluster, even when they are collected again, left the cluster.
     */
    public void testANodeThatLeftARemoteClusterIsNotFound() {
        PlainActionFuture<DiscoveryNode> left = find(new FetchNode(REMOTE, "remote-9"));
        remote.complete(0, Map.of("remote-1", REMOTE_NODE));
        remote.complete(1, Map.of("remote-1", REMOTE_NODE));

        NoNodeAvailableException e = expectThrows(NoNodeAvailableException.class, left::actionGet);
        assertThat(e.getMessage(), equalTo("node [remote-9] left the cluster [remote]"));
        assertThat("collected once more, no more", remote.started, hasSize(2));
    }

    /**
     * A collection that fails fails the requests that wait for it, and the next request collects again.
     */
    public void testAFailedCollectionIsForgotten() {
        PlainActionFuture<DiscoveryNode> failed = find(new FetchNode(REMOTE, "remote-1"));
        NoSuchRemoteClusterException unknown = new NoSuchRemoteClusterException(REMOTE);
        remote.fail(0, unknown);

        assertThat(expectThrows(NoSuchRemoteClusterException.class, failed::actionGet), sameInstance(unknown));
        PlainActionFuture<DiscoveryNode> retried = find(new FetchNode(REMOTE, "remote-1"));
        remote.complete(1, Map.of("remote-1", REMOTE_NODE));
        assertThat(retried.actionGet(), sameInstance(REMOTE_NODE));
    }

    /**
     * A collector that throws fails the request like a collection that fails.
     */
    public void testACollectorThatThrowsFailsTheRequest() {
        IllegalStateException notAClient = new IllegalStateException("this node isn't a remote cluster client");
        FetchNodeDirectory throwing = new FetchNodeDirectory(
            () -> DiscoveryNodes.EMPTY_NODES,
            (clusterAlias, listener) -> { throw notAClient; },
            threadContext
        );
        PlainActionFuture<DiscoveryNode> found = new PlainActionFuture<>();

        throwing.find(new FetchNode(REMOTE, "remote-1"), found);

        assertThat(expectThrows(IllegalStateException.class, found::actionGet), sameInstance(notAClient));
    }

    public void testPrepareStartsCollectingBeforeTheFirstRequest() {
        directory.prepare(REMOTE);
        assertThat(remote.started, equalTo(List.of(REMOTE)));

        remote.complete(0, Map.of("remote-1", REMOTE_NODE));
        directory.prepare(REMOTE);
        assertThat(find(new FetchNode(REMOTE, "remote-1")).actionGet(), sameInstance(REMOTE_NODE));
        assertThat(remote.started, hasSize(1));
    }

    public void testPrepareDoesNothingForTheLocalCluster() {
        directory.prepare(FetchNode.local("local-1").clusterAlias());
        assertThat(remote.started, hasSize(0));
    }

    /**
     * The nodes serve the queries of every user, so the user who asked first takes no part in collecting them, and the
     * listener of each request runs in the thread context of its own caller.
     */
    public void testEveryRequestKeepsTheThreadContextOfItsCaller() {
        PlainActionFuture<String> first = new PlainActionFuture<>();
        PlainActionFuture<String> second = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("user", "first");
            directory.find(new FetchNode(REMOTE, "remote-1"), first.map(node -> threadContext.getHeader("user")));
        }
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("user", "second");
            directory.find(new FetchNode(REMOTE, "remote-1"), second.map(node -> threadContext.getHeader("user")));
        }

        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader("user", "collector");
            remote.complete(0, Map.of("remote-1", REMOTE_NODE));
        }

        assertThat("no user collects the nodes", remote.headersWhenStarted.getFirst(), equalTo(""));
        assertThat(first.actionGet(), equalTo("first"));
        assertThat(second.actionGet(), equalTo("second"));
    }

    private PlainActionFuture<DiscoveryNode> find(FetchNode node) {
        PlainActionFuture<DiscoveryNode> found = new PlainActionFuture<>();
        directory.find(node, found);
        return found;
    }

    /**
     * Collections of the nodes of remote clusters that each test completes itself, in the order they started.
     */
    private class RemoteCollections implements FetchNodeDirectory.RemoteNodes {
        final List<String> started = new CopyOnWriteArrayList<>();
        final List<String> headersWhenStarted = new CopyOnWriteArrayList<>();
        final List<ActionListener<Function<String, DiscoveryNode>>> pending = new CopyOnWriteArrayList<>();

        @Override
        public void collect(String clusterAlias, ActionListener<Function<String, DiscoveryNode>> listener) {
            started.add(clusterAlias);
            String user = threadContext.getHeader("user");
            headersWhenStarted.add(user == null ? "" : user);
            pending.add(listener);
        }

        void complete(int collection, Map<String, DiscoveryNode> nodes) {
            pending.get(collection).onResponse(nodes::get);
        }

        void fail(int collection, Exception e) {
            pending.get(collection).onFailure(e);
        }
    }
}
