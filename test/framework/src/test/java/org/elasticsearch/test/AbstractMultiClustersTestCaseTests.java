/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.plugins.Plugin;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class AbstractMultiClustersTestCaseTests extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster-a";
    private static final AtomicReference<Exception> nextPluginCloseFailure = new AtomicReference<>();

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(FailOnClosePlugin.class);
    }

    public void testFailedCloseStillClearsClustersAndReportsLeakedPages() throws Exception {
        Exception closeFailure = failNextPluginClose();
        new MockPageCacheRecycler(Settings.EMPTY).bytePage(false);

        Exception e = expectThrows(Exception.class, AbstractMultiClustersTestCase::stopClusters);
        assertThat(e, sameInstance(closeFailure));
        assertThat(e.getSuppressed(), arrayWithSize(1));
        assertThat(e.getSuppressed()[0], instanceOf(AssertionError.class));
        assertThat(e.getSuppressed()[0].getMessage(), equalTo("1 pages have not been released"));

        MockPageCacheRecycler.ensureAllPagesAreReleased();
        assertClustersRestartAndServeRequests();
    }

    public void testFailedCloseWithoutLeakedPagesStillClearsClusters() throws Exception {
        Exception closeFailure = failNextPluginClose();

        Exception e = expectThrows(Exception.class, AbstractMultiClustersTestCase::stopClusters);
        assertThat(e, sameInstance(closeFailure));
        assertThat(e.getSuppressed(), emptyArray());

        assertClustersRestartAndServeRequests();
    }

    public void testLeakedPagesAfterSuccessfulCloseStillClearsClusters() throws Exception {
        new MockPageCacheRecycler(Settings.EMPTY).bytePage(false);

        AssertionError e = expectThrows(AssertionError.class, AbstractMultiClustersTestCase::stopClusters);
        assertThat(e.getMessage(), equalTo("1 pages have not been released"));

        MockPageCacheRecycler.ensureAllPagesAreReleased();
        assertClustersRestartAndServeRequests();
    }

    public void testCloseAllClosesRemainingClustersAfterAssertionError() {
        AtomicBoolean lastClosed = new AtomicBoolean();
        List<Closeable> closeables = List.of(() -> { throw new AssertionError("node did not close"); }, () -> lastClosed.set(true));

        AssertionError e = expectThrows(AssertionError.class, () -> ClusterGroup.closeAll(closeables));
        assertThat(e.getMessage(), equalTo("node did not close"));
        assertThat(e.getSuppressed(), emptyArray());
        assertTrue(lastClosed.get());
    }

    public void testCloseAllAddsLaterFailuresAsSuppressed() {
        AtomicBoolean lastClosed = new AtomicBoolean();
        List<Closeable> closeables = List.of(() -> { throw new IllegalStateException("shards still open"); }, () -> {
            throw new AssertionError("node did not close");
        }, () -> lastClosed.set(true));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> ClusterGroup.closeAll(closeables));
        assertThat(e.getMessage(), equalTo("shards still open"));
        assertThat(e.getSuppressed(), arrayWithSize(1));
        assertThat(e.getSuppressed()[0], instanceOf(AssertionError.class));
        assertThat(e.getSuppressed()[0].getMessage(), equalTo("node did not close"));
        assertTrue(lastClosed.get());
    }

    /**
     * Makes the next plugin close throw an {@link IOException} or a {@link RuntimeException}, the failures that
     * {@code Node#close} collects while still closing the rest of the node. An {@link AssertionError} would stop
     * {@code Node#close} before it shuts down the node's thread pool, leaking its threads into the suite.
     */
    private static Exception failNextPluginClose() {
        Exception failure = randomBoolean()
            ? new IOException("simulated close failure")
            : new IllegalStateException("simulated close failure");
        nextPluginCloseFailure.set(failure);
        return failure;
    }

    private void assertClustersRestartAndServeRequests() throws Exception {
        startClusters();
        for (String clusterAlias : List.of(LOCAL_CLUSTER, REMOTE_CLUSTER)) {
            ClusterHealthResponse health = client(clusterAlias).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).get();
            assertThat(health.isTimedOut(), is(false));
        }
    }

    /**
     * Makes {@link AbstractMultiClustersTestCase#stopClusters()} fail the way it does when a node's close throws, e.g. when
     * {@code Node#awaitClose} finds shards that a test leaked.
     */
    public static class FailOnClosePlugin extends Plugin {
        @Override
        public void close() throws IOException {
            Exception failure = nextPluginCloseFailure.getAndSet(null);
            if (failure instanceof IOException e) {
                throw e;
            }
            if (failure instanceof RuntimeException e) {
                throw e;
            }
            assert failure == null : failure;
        }
    }
}
