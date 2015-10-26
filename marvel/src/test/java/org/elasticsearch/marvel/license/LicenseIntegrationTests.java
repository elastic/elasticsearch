/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.marvel.MarvelPlugin;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

@ClusterScope(scope = SUITE, transportClientRatio = 0, numClientNodes = 0)
public class LicenseIntegrationTests extends MarvelIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        if (shieldEnabled) {
            return Arrays.asList(MockLicensePlugin.class, MarvelPlugin.class, ShieldPlugin.class);
        }
        return Arrays.asList(MockLicensePlugin.class, MarvelPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .build();
    }

    public void testEnableDisableLicense() {
        assertThat(getLicensee().getStatus().getLicenseState(), isOneOf(LicenseState.ENABLED, LicenseState.GRACE_PERIOD));
        assertThat(getLicensee().collectionEnabled(), is(true));
        disableLicensing();

        assertThat(getLicensee().getStatus().getLicenseState(), equalTo(LicenseState.DISABLED));
        assertThat(getLicensee().collectionEnabled(), is(false));
        enableLicensing();

        assertThat(getLicensee().getStatus().getLicenseState(), isOneOf(LicenseState.ENABLED, LicenseState.GRACE_PERIOD));
        assertThat(getLicensee().collectionEnabled(), is(true));
    }

    private MarvelLicensee getLicensee() {
        MarvelLicensee marvelLicensee = internalCluster().getInstance(MarvelLicensee.class);
        assertNotNull(marvelLicensee);
        return marvelLicensee;
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
            bind(LicenseeRegistry.class).to(MockLicenseService.class);
            bind(LicensesManagerService.class).to(MockLicenseService.class);
        }
    }

    public static class MockLicenseService extends AbstractComponent implements LicenseeRegistry, LicensesManagerService {

        private final List<Licensee> licensees = new ArrayList<>();

        @Inject
        public MockLicenseService(Settings settings) {
            super(settings);
            enable();
        }

        @Override
        public void register(Licensee licensee) {
            licensees.add(licensee);
            enable();
        }

        public void enable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(License.OperationMode.BASIC, randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));
            }
        }

        public void disable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(License.OperationMode.BASIC, LicenseState.DISABLED));
            }
        }

        @Override
        public List<String> licenseesWithState(LicenseState state) {
            List<String> licenseesWithState = new ArrayList<>();
            for (Licensee licensee : licensees) {
                licenseesWithState.add(licensee.id());
            }
            return licenseesWithState;
        }

        @Override
        public License getLicense() {
            return null;
        }
    }
}
