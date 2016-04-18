/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SysGlobals;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.Licensing;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.InternalClient;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, randomDynamicTemplates = false, transportClientRatio = 0.0)
public abstract class AbstractCollectorTestCase extends MarvelIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalXPackPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), false)
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .build();
    }

    @Before
    public void ensureLicenseIsEnabled() {
        enableLicense();
    }

    public InternalClient securedClient() {
        return internalCluster().getInstance(InternalClient.class);
    }

    public InternalClient securedClient(String nodeId) {
        return internalCluster().getInstance(InternalClient.class, nodeId);
    }

    protected void assertCanCollect(AbstractCollector collector) {
        assertNotNull(collector);
        assertTrue("collector [" + collector.name() + "] should be able to collect data", collector.shouldCollect());
        Collection results = collector.collect();
        assertNotNull(results);
    }

    protected void assertCannotCollect(AbstractCollector collector) {
        assertNotNull(collector);
        assertFalse("collector [" + collector.name() + "] should not be able to collect data", collector.shouldCollect());
        Collection results = collector.collect();
        assertTrue(results == null || results.isEmpty());
    }

    private static License createTestingLicense(long issueDate, long expiryDate) {
        return License.builder()
                .expiryDate(expiryDate)
                .issueDate(issueDate)
                .issuedTo("AbstractCollectorTestCase")
                .issuer("test")
                .maxNodes(Integer.MAX_VALUE)
                .signature("_signature")
                .type("trial")
                .uid(String.valueOf(RandomizedTest.systemPropertyAsInt(SysGlobals.CHILDVM_SYSPROP_JVM_ID, 0)) +
                        System.identityHashCode(AbstractCollectorTestCase.class))
                .build();
    }

    protected static void enableLicense() {
        long issueDate = System.currentTimeMillis();
        long expiryDate = issueDate + randomDaysInMillis();

        final License license = createTestingLicense(issueDate, expiryDate);
        for (LicenseServiceForCollectors service : internalCluster().getInstances(LicenseServiceForCollectors.class)) {
            service.onChange(license.operationMode(), LicenseState.ENABLED);
        }
        for (LicensesManagerServiceForCollectors service : internalCluster().getInstances(LicensesManagerServiceForCollectors.class)) {
            service.update(license);
        }
    }

    protected static void beginGracefulPeriod() {
        long expiryDate = System.currentTimeMillis() + timeValueMinutes(10).millis();
        long issueDate = expiryDate - randomDaysInMillis();

        final License license = createTestingLicense(issueDate, expiryDate);
        for (LicenseServiceForCollectors service : internalCluster().getInstances(LicenseServiceForCollectors.class)) {
            service.onChange(license.operationMode(), LicenseState.GRACE_PERIOD);
        }
        for (LicensesManagerServiceForCollectors service : internalCluster().getInstances(LicensesManagerServiceForCollectors.class)) {
            service.update(license);
        }
    }

    protected static void endGracefulPeriod() {
        long expiryDate = System.currentTimeMillis() - MonitoringSettings.MAX_LICENSE_GRACE_PERIOD.millis() - timeValueMinutes(10).millis();
        long issueDate = expiryDate - randomDaysInMillis();

        final License license = createTestingLicense(issueDate, expiryDate);
        for (LicenseServiceForCollectors service : internalCluster().getInstances(LicenseServiceForCollectors.class)) {
            service.onChange(license.operationMode(), LicenseState.DISABLED);
        }
        for (LicensesManagerServiceForCollectors service : internalCluster().getInstances(LicensesManagerServiceForCollectors.class)) {
            service.update(license);
        }
    }

    protected static void disableLicense() {
        long expiryDate = System.currentTimeMillis() - MonitoringSettings.MAX_LICENSE_GRACE_PERIOD.millis() - randomDaysInMillis();
        long issueDate = expiryDate - randomDaysInMillis();

        final License license = createTestingLicense(issueDate, expiryDate);
        for (LicenseServiceForCollectors service : internalCluster().getInstances(LicenseServiceForCollectors.class)) {
            service.onChange(license.operationMode(), LicenseState.DISABLED);
        }
        for (LicensesManagerServiceForCollectors service : internalCluster().getInstances(LicensesManagerServiceForCollectors.class)) {
            service.update(license);
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
        return assertBusy(() -> {
            ClusterBlocks clusterBlocks =
                    client(nodeId).admin().cluster().prepareState().setLocal(true).execute().actionGet().getState().blocks();
            assertTrue(clusterBlocks.global().isEmpty());
            assertTrue(clusterBlocks.indices().values().isEmpty());
            return true;
        }, 30L, TimeUnit.SECONDS);
    }

    public static class InternalLicensing extends Licensing {

        public InternalLicensing() {
            super(Settings.EMPTY);
        }

        @Override
        public Collection<Module> nodeModules() {
            return Collections.<Module>singletonList(new AbstractModule() {

                @Override
                protected void configure() {
                    bind(LicenseServiceForCollectors.class).asEagerSingleton();
                    bind(LicenseeRegistry.class).to(LicenseServiceForCollectors.class);
                    bind(LicensesManagerServiceForCollectors.class).asEagerSingleton();
                    bind(LicensesManagerService.class).to(LicensesManagerServiceForCollectors.class);
                }
            });
        }

        @Override
        public void onModule(NetworkModule module) {
        }

        @Override
        public void onModule(ActionModule module) {
        }

        @Override
        public Collection<Class<? extends LifecycleComponent>> nodeServices() {
            return Collections.emptyList();
        }
    }

    public static class InternalXPackPlugin extends XPackPlugin {

        public InternalXPackPlugin(Settings settings) {
            super(settings);
            licensing = new InternalLicensing();
        }
    }

    public static class LicenseServiceForCollectors extends AbstractComponent implements LicenseeRegistry {

        private final List<Licensee> licensees = new ArrayList<>();

        @Inject
        public LicenseServiceForCollectors(Settings settings) {
            super(settings);
        }

        @Override
        public void register(Licensee licensee) {
            licensees.add(licensee);
        }

        public void onChange(License.OperationMode operationMode, LicenseState state) {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(operationMode, state));
            }
        }
    }

    public static class LicensesManagerServiceForCollectors implements LicensesManagerService {

        private volatile License license;

        @Override
        public List<String> licenseesWithState(LicenseState state) {
            return null;
        }

        @Override
        public License getLicense() {
            return license;
        }

        public synchronized void update(License license) {
            this.license = license;
        }
    }
}
