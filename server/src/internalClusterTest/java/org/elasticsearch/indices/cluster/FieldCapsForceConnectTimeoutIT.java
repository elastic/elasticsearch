/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.cluster;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.TransportFieldCapabilitiesAction;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class FieldCapsForceConnectTimeoutIT extends AbstractMultiClustersTestCase {
    private static final String LINKED_CLUSTER_1 = "cluster-a";
    private static final String LINKED_CLUSTER_2 = "cluster-b";

    public static class ForceConnectTimeoutPlugin extends Plugin implements ClusterPlugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(ForceConnectTimeoutSetting);
        }
    }

    private static final Setting<String> ForceConnectTimeoutSetting = Setting.simpleString(
        "search.ccs.force_connect_timeout",
        Setting.Property.NodeScope
    );

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(LINKED_CLUSTER_1, LINKED_CLUSTER_2);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.appendToCopy(super.nodePlugins(clusterAlias), ForceConnectTimeoutPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        /*
         * This is the setting that controls how long TransportFieldCapabilitiesAction will wait for establishing a connection
         * with a remote. At present, we set it to low 1s to prevent stalling the test for too long -- this is consistent
         * with what we've done in other tests.
         */
        return Settings.builder().put(super.nodeSettings()).put("search.ccs.force_connect_timeout", "1s").build();
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(LINKED_CLUSTER_1, true, LINKED_CLUSTER_2, true);
    }

    public void testTimeoutSetting() {
        var latch = new CountDownLatch(1);
        for (String localNodeName : cluster(LOCAL_CLUSTER).getNodeNames()) {
            MockTransportService localMts = (MockTransportService) cluster(LOCAL_CLUSTER).getInstance(
                TransportService.class,
                localNodeName
            );
            for (String remoteNodeName : cluster(LINKED_CLUSTER_1).getNodeNames()) {
                TransportService remoteTs = cluster(LINKED_CLUSTER_1).getInstance(TransportService.class, remoteNodeName);
                localMts.addConnectBehavior(remoteTs, ((transport, discoveryNode, profile, listener) -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }

                    transport.openConnection(discoveryNode, profile, listener);
                }));
            }
        }

        // Add some dummy data to prove we are communicating fine with the remote.
        assertAcked(client(LINKED_CLUSTER_1).admin().indices().prepareCreate("test-index"));
        client(LINKED_CLUSTER_1).prepareIndex("test-index").setSource("sample-field", "sample-value").get();
        client(LINKED_CLUSTER_1).admin().indices().prepareRefresh("test-index").get();

        /*
         * Do a full restart so that our custom connect behaviour takes effect since it does not apply to
         * pre-existing connections -- they're already established by the time this test runs.
         */
        try {
            cluster(LINKED_CLUSTER_1).fullRestart();
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            var fieldCapsRequest = new FieldCapabilitiesRequest();
            /*
             * We have an origin and 2 linked clusters but will target only the one that we stalled.
             * This is because when the timeout kicks in, and we move on from the stalled cluster, we do not want
             * the error to be a top-level error. Rather, it must be present in the response object under "failures".
             * All other errors are free to be top-level errors though.
             */
            fieldCapsRequest.indices(LINKED_CLUSTER_1 + ":*");
            fieldCapsRequest.fields("foo", "bar", "baz");
            var result = safeGet(client().execute(TransportFieldCapabilitiesAction.TYPE, fieldCapsRequest));

            var failures = result.getFailures();
            assertThat(failures.size(), Matchers.is(1));

            var failure = failures.getFirst();
            assertThat(failure.getIndices().length, Matchers.is(1));
            assertThat(failure.getIndices()[0], Matchers.equalTo("cluster-a:*"));
            // Outer wrapper that gets unwrapped in ExceptionsHelper.isRemoteUnavailableException().
            assertThat(
                failure.getException().toString(),
                Matchers.containsString("java.lang.IllegalStateException: Unable to open any connections")
            );

            // The actual error that is thrown by the subscribable listener when a linked cluster could not be talked to.
            assertThat(failure.getException().getCause(), Matchers.instanceOf(ElasticsearchTimeoutException.class));
            assertThat(ExceptionsHelper.isRemoteUnavailableException(failure.getException()), Matchers.is(true));

            latch.countDown();
            result.decRef();
        }
    }
}
