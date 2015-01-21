/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Before;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.license.plugin.core.LicensesService.LicensesUpdateResponse;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractLicensesServiceTests extends AbstractLicensesIntegrationTests {

    private static String node = null;
    private static String[] nodes;

    @Before
    public void beforeTest() throws Exception {
        wipeAllLicenses();

        DiscoveryNodes discoveryNodes = masterClusterService().state().getNodes();
        Set<String> dataNodeSet = new HashSet<>();
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            if (discoveryNode.dataNode()) {
                dataNodeSet.add(discoveryNode.getName());
            }
        }
        nodes = dataNodeSet.toArray(new String[dataNodeSet.size()]);
        node = nodes[randomIntBetween(0, nodes.length - 1)];
    }

    protected void registerAndAckSignedLicenses(final LicensesManagerService masterLicensesManagerService, final List<License> license, final LicensesStatus expectedStatus) {
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().licenses(license);
        LicensesService.PutLicenseRequestHolder requestHolder = new LicensesService.PutLicenseRequestHolder(putLicenseRequest, "test");
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(false);
        masterLicensesManagerService.registerLicenses(requestHolder, new ActionListener<LicensesUpdateResponse>() {
            @Override
            public void onResponse(LicensesUpdateResponse licensesUpdateResponse) {
                if (licensesUpdateResponse.isAcknowledged() && licensesUpdateResponse.status() == expectedStatus) {
                    success.set(true);
                }
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        assertThat("register license(s) failed", success.get(), equalTo(true));
    }

    protected Action registerWithTrialLicense(final LicensesClientService clientService, final LicensesClientService.Listener clientListener, final String feature, final TimeValue expiryDuration) {
        return new Action(new Runnable() {
            @Override
            public void run() {
                clientService.register(feature, new LicensesService.TrialLicenseOptions(expiryDuration, 10), Collections.<LicensesService.ExpirationCallback>emptyList(),
                        clientListener);

                // invoke clusterChanged event to flush out pendingRegistration
                LicensesService licensesService = (LicensesService) clientService;
                ClusterChangedEvent event = new ClusterChangedEvent("", clusterService().state(), clusterService().state());
                licensesService.clusterChanged(event);
            }
        }, 0, 1, "should trigger onEnable for " + feature + " once [trial license]");
    }

    protected Action registerWithEventNotification(final LicensesClientService clientService, final LicensesClientService.Listener clientListener, final String feature, final TimeValue expiryDuration, final Collection<LicensesService.ExpirationCallback> expirationCallbacks) {
        return new Action(new Runnable() {
            @Override
            public void run() {
                clientService.register(feature, new LicensesService.TrialLicenseOptions(expiryDuration, 10), expirationCallbacks,
                        clientListener);

                // invoke clusterChanged event to flush out pendingRegistration
                LicensesService licensesService = (LicensesService) clientService;
                ClusterChangedEvent event = new ClusterChangedEvent("", clusterService().state(), clusterService().state());
                licensesService.clusterChanged(event);
            }
        }, 0, 1, "should trigger onEnable for " + feature + " once [trial license]");
    }

    protected class TestTrackingClientListener implements LicensesClientService.Listener {
        CountDownLatch enableLatch;
        CountDownLatch disableLatch;
        AtomicBoolean enabled = new AtomicBoolean(false);

        final boolean track;
        final String featureName;

        public TestTrackingClientListener(String featureName) {
            this(featureName, true);
        }

        public TestTrackingClientListener(String featureName, boolean track) {
            this.track = track;
            this.featureName = featureName;
        }

        public synchronized void latch(CountDownLatch enableLatch, CountDownLatch disableLatch) {
            this.enableLatch = enableLatch;
            this.disableLatch = disableLatch;
        }

        @Override
        public void onEnabled(License license) {
            assertNotNull(license);
            assertThat(license.feature(), equalTo(featureName));
            enabled.set(true);
            if (track) {
                this.enableLatch.countDown();
            }
        }

        @Override
        public void onDisabled(License license) {
            assertNotNull(license);
            assertThat(license.feature(), equalTo(featureName));
            enabled.set(false);
            if (track) {
                this.disableLatch.countDown();
            }
        }
    }

    protected class Action {
        final int expectedDisabledCount;
        final int expectedEnabledCount;
        final TimeValue timeout;
        final Runnable action;
        final String msg;

        protected Action(Runnable action, int expectedEnabledCount, int expectedDisabledCount, String msg) {
            this(action, expectedEnabledCount, expectedDisabledCount, TimeValue.timeValueSeconds(1), msg);
        }

        protected Action(Runnable action, int expectedDisabledCount, int expectedEnabledCount, TimeValue timeout, String msg) {
            this.expectedDisabledCount = expectedDisabledCount;
            this.expectedEnabledCount = expectedEnabledCount;
            this.action = action;
            this.timeout = timeout;
            this.msg = msg;
        }

        public void run() {
            action.run();
        }
    }


    protected LicensesManagerService masterLicensesManagerService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(LicensesManagerService.class, clients.getMasterName());
    }

    protected LicensesClientService licensesClientService() {
        return internalCluster().getInstance(LicensesClientService.class, node);
    }

    protected LicensesService randomLicensesService() {
        String randomNode = randomFrom(nodes);
        return internalCluster().getInstance(LicensesService.class, randomNode);
    }

    protected static ClusterService masterClusterService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(ClusterService.class, clients.getMasterName());
    }
}
