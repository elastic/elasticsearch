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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AbstractMultiClustersTestCaseTests extends AbstractMultiClustersTestCase {

    private static final AtomicBoolean failNextPluginClose = new AtomicBoolean();

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of("cluster-a");
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return List.of(FailOnClosePlugin.class);
    }

    public void testFailedCloseStillClearsClustersAndReportsLeakedPages() throws Exception {
        failNextPluginClose.set(true);
        new MockPageCacheRecycler(Settings.EMPTY).bytePage(false);

        IllegalStateException e = expectThrows(IllegalStateException.class, AbstractMultiClustersTestCase::stopClusters);
        assertThat(e.getMessage(), equalTo("simulated close failure"));
        assertThat(e.getSuppressed(), arrayWithSize(1));
        assertThat(e.getSuppressed()[0], instanceOf(AssertionError.class));
        assertThat(e.getSuppressed()[0].getMessage(), equalTo("1 pages have not been released"));

        MockPageCacheRecycler.ensureAllPagesAreReleased();
        assertClustersRestartAndServeRequests();
    }

    public void testFailedCloseWithoutLeakedPagesStillClearsClusters() throws Exception {
        failNextPluginClose.set(true);

        IllegalStateException e = expectThrows(IllegalStateException.class, AbstractMultiClustersTestCase::stopClusters);
        assertThat(e.getMessage(), equalTo("simulated close failure"));
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

    private void assertClustersRestartAndServeRequests() throws Exception {
        startClusters();
        for (String clusterAlias : List.of(LOCAL_CLUSTER, "cluster-a")) {
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
        public void close() {
            if (failNextPluginClose.compareAndSet(true, false)) {
                throw new IllegalStateException("simulated close failure");
            }
        }
    }
}
