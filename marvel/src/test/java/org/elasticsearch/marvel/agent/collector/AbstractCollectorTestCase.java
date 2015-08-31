/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SysGlobals;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.license.LicenseIntegrationTests;
import org.elasticsearch.marvel.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Before;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, randomDynamicTemplates = false, transportClientRatio = 0.0)
public class AbstractCollectorTestCase extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LicensePluginForCollectors.class, MarvelPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Before
    public void ensureLicenseIsEnabled() {
        enableLicense();
    }

    protected void assertCanCollect(AbstractCollector collector) {
        assertNotNull(collector);
        assertTrue("collector [" + collector.name() + "] should be able to collect data", collector.canCollect());
        Collection results = collector.collect();
        assertTrue(results != null && !results.isEmpty());
    }

    protected void assertCannotCollect(AbstractCollector collector) {
        assertNotNull(collector);
        assertFalse("collector [" + collector.name() + "] should not be able to collect data", collector.canCollect());
        Collection results = collector.collect();
        assertTrue(results == null || results.isEmpty());
    }

    private static License createTestingLicense(long issueDate, long expiryDate) {
        return License.builder()
                .feature(LicenseService.FEATURE_NAME)
                .expiryDate(expiryDate)
                .issueDate(issueDate)
                .issuedTo("AbstractCollectorTestCase")
                .issuer("test")
                .maxNodes(Integer.MAX_VALUE)
                .signature("_signature")
                .type("standard")
                .subscriptionType("all_is_good")
                .uid(String.valueOf(RandomizedTest.systemPropertyAsInt(SysGlobals.CHILDVM_SYSPROP_JVM_ID, 0)) + System.identityHashCode(LicenseIntegrationTests.class))
                .build();
    }

    protected static void enableLicense() {
        long issueDate = System.currentTimeMillis();
        long expiryDate = issueDate + randomDaysInMillis();

        final License license = createTestingLicense(issueDate, expiryDate);
        for (LicenseServiceForCollectors service : internalCluster().getInstances(LicenseServiceForCollectors.class)) {
            service.enable(license);
        }
    }

    protected static void beginGracefulPeriod() {
        long expiryDate = System.currentTimeMillis() + TimeValue.timeValueMinutes(10).millis();
        long issueDate = expiryDate - randomDaysInMillis();

        final License license = createTestingLicense(issueDate, expiryDate);
        for (LicenseServiceForCollectors service : internalCluster().getInstances(LicenseServiceForCollectors.class)) {
            service.disable(license);
        }
    }

    protected static void endGracefulPeriod() {
        long expiryDate = System.currentTimeMillis() - MarvelSettings.MAX_LICENSE_GRACE_PERIOD.millis() - TimeValue.timeValueMinutes(10).millis();
        long issueDate = expiryDate - randomDaysInMillis();

        final License license = createTestingLicense(issueDate, expiryDate);
        for (LicenseServiceForCollectors service : internalCluster().getInstances(LicenseServiceForCollectors.class)) {
            service.disable(license);
        }
    }

    protected static void disableLicense() {
        long expiryDate = System.currentTimeMillis() - MarvelSettings.MAX_LICENSE_GRACE_PERIOD.millis() - randomDaysInMillis();
        long issueDate = expiryDate - randomDaysInMillis();

        final License license = createTestingLicense(issueDate, expiryDate);
        for (LicenseServiceForCollectors service : internalCluster().getInstances(LicenseServiceForCollectors.class)) {
            service.disable(license);
        }
    }

    private static long randomDaysInMillis() {
        return TimeValue.timeValueHours(randomIntBetween(1, 30) * 24).millis();
    }

    public void waitForNoBlocksOnNodes() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (String nodeId : internalCluster().getNodeNames()) {
                    try {
                        assertTrue(waitForNoBlocksOnNode(nodeId));
                    } catch (Exception e) {
                        fail("failed to wait for no blocks on node [" + nodeId + "]: " + e.getMessage());
                    }
                }
            }
        });
    }

    public boolean waitForNoBlocksOnNode(final String nodeId) throws Exception {
        return assertBusy(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ClusterBlocks clusterBlocks = client(nodeId).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState().blocks();
                assertTrue(clusterBlocks.global().isEmpty());
                assertTrue(clusterBlocks.indices().values().isEmpty());
                return true;
            }
        }, 30L, TimeUnit.SECONDS);
    }

    public static class LicensePluginForCollectors extends Plugin {

        public static final String NAME = "internal-test-licensing";

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public String description() {
            return name();
        }

        @Override
        public Collection<Module> nodeModules() {
            return Collections.<Module>singletonList(new AbstractModule(){

                @Override
                protected void configure() {
                    bind(LicenseServiceForCollectors.class).asEagerSingleton();
                    bind(LicensesClientService.class).to(LicenseServiceForCollectors.class);
                }
            });
        }
    }

    public static class LicenseServiceForCollectors extends AbstractComponent implements LicensesClientService {

        private final List<Listener> listeners = new ArrayList<>();

        @Inject
        public LicenseServiceForCollectors(Settings settings) {
            super(settings);
        }

        @Override
        public void register(String feature, TrialLicenseOptions trialLicenseOptions, Collection<ExpirationCallback> expirationCallbacks, AcknowledgementCallback acknowledgementCallback, Listener listener) {
            listeners.add(listener);
        }

        public void enable(License license) {
            for (Listener listener : listeners) {
                listener.onEnabled(license);
            }
        }

        public void disable(License license) {
            for (Listener listener : listeners) {
                listener.onDisabled(license);
            }
        }
    }
}
