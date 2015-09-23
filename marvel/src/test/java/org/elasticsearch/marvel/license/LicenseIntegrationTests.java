/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SysGlobals;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.mode.Mode;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.util.*;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = SUITE, transportClientRatio = 0, numClientNodes = 0)
public class LicenseIntegrationTests extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    @Test
    public void testEnableDisableLicense() {
        assertThat(getLicenseService().mode(), equalTo(Mode.STANDARD));
        assertThat(getLicenseService().enabled(), is(true));
        assertThat(getLicenseService().expiryDate(), greaterThan(0L));
        disableLicensing();

        assertThat(getLicenseService().mode(), equalTo(Mode.LITE));
        assertThat(getLicenseService().enabled(), is(false));
        assertThat(getLicenseService().expiryDate(), greaterThan(0L));
        enableLicensing();

        assertThat(getLicenseService().mode(), equalTo(Mode.STANDARD));
        assertThat(getLicenseService().enabled(), is(true));
        assertThat(getLicenseService().expiryDate(), greaterThan(0L));
    }

    private LicenseService getLicenseService() {
        LicenseService licenseService = internalCluster().getInstance(LicenseService.class);
        assertNotNull(licenseService);
        return licenseService;
    }

    public static void disableLicensing() {
        for (MockLicenseService service : internalCluster().getInstances(MockLicenseService.class)) {
            service.disable();
        }
    }

    public static void enableLicensing() {
        for (MockLicenseService service : internalCluster().getInstances(MockLicenseService.class)) {
            service.enable();
        }
    }

    public static class MockLicensePlugin extends Plugin {

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
            return Collections.<Module>singletonList(new InternalLicenseModule());
        }
    }

    public static class InternalLicenseModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(MockLicenseService.class).asEagerSingleton();
            bind(LicensesClientService.class).to(MockLicenseService.class);
        }
    }

    public static class MockLicenseService extends AbstractComponent implements LicensesClientService {

        static final License DUMMY_LICENSE = License.builder()
                .feature(LicenseService.FEATURE_NAME)
                .expiryDate(System.currentTimeMillis())
                .issueDate(System.currentTimeMillis())
                .issuedTo("LicensingTests")
                .issuer("test")
                .maxNodes(Integer.MAX_VALUE)
                .signature("_signature")
                .type("standard")
                .subscriptionType("all_is_good")
                .uid(String.valueOf(RandomizedTest.systemPropertyAsInt(SysGlobals.CHILDVM_SYSPROP_JVM_ID, 0)) + System.identityHashCode(LicenseIntegrationTests.class))
                .build();

        private final List<Listener> listeners = new ArrayList<>();

        @Inject
        public MockLicenseService(Settings settings) {
            super(settings);
            enable();
        }

        @Override
        public void register(String s, LicensesService.TrialLicenseOptions trialLicenseOptions, Collection<LicensesService.ExpirationCallback> collection, AcknowledgementCallback acknowledgementCallback, Listener listener) {
            listeners.add(listener);
            enable();
        }

        public void enable() {
            // enabled all listeners (incl. shield)
            for (Listener listener : listeners) {
                listener.onEnabled(DUMMY_LICENSE);
            }
        }

        public void disable() {
            // only disable watcher listener (we need shield to work)
            for (Listener listener : listeners) {
                listener.onDisabled(DUMMY_LICENSE);
            }
        }
    }
}
