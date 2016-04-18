/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.license;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.Licensing;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.marvel.MonitoringLicensee;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.XPackPlugin;

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
        return Arrays.asList(InternalXPackPlugin.class);
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

    private MonitoringLicensee getLicensee() {
        MonitoringLicensee licensee = internalCluster().getInstance(MonitoringLicensee.class);
        assertNotNull(licensee);
        return licensee;
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

    public static class MockLicensing extends Licensing {

        public MockLicensing() {
            super(Settings.EMPTY);
        }

        @Override
        public Collection<Module> nodeModules() {
            return Collections.<Module>singletonList(new InternalLicenseModule());
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
                licensee.onChange(new Licensee.Status(License.OperationMode.BASIC,
                        randomBoolean() ? LicenseState.ENABLED : LicenseState.GRACE_PERIOD));
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

    public static class InternalXPackPlugin extends XPackPlugin {
        public InternalXPackPlugin(Settings settings) {
            super(settings);
            licensing = new MockLicensing();
        }
    }
}
