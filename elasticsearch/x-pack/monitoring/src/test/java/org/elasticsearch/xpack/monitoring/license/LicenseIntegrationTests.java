/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.license;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.Licensing;
import org.elasticsearch.license.plugin.core.LicenseService;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.graph.GraphLicensee;
import org.elasticsearch.xpack.monitoring.MonitoringLicensee;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.elasticsearch.xpack.security.SecurityLicenseState;
import org.elasticsearch.xpack.support.clock.Clock;
import org.elasticsearch.xpack.watcher.WatcherLicensee;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

@ClusterScope(scope = SUITE, transportClientRatio = 0, numClientNodes = 0)
public class LicenseIntegrationTests extends MonitoringIntegTestCase {
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
            return Collections.singletonList(b -> b.bind(LicenseService.class).to(MockLicenseService.class));
        }

        @Override
        public Collection<Object> createComponents(ClusterService clusterService, Clock clock, Environment environment,
                                                   ResourceWatcherService resourceWatcherService,
                                                   SecurityLicenseState securityLicenseState) {
            WatcherLicensee watcherLicensee = new WatcherLicensee(settings);
            MonitoringLicensee monitoringLicensee = new MonitoringLicensee(settings);
            GraphLicensee graphLicensee = new GraphLicensee(settings);
            LicenseService licenseService = new MockLicenseService(settings, environment, resourceWatcherService,
                    Arrays.asList(watcherLicensee, monitoringLicensee, graphLicensee));
            return Arrays.asList(licenseService, watcherLicensee, monitoringLicensee, graphLicensee);
        }

        @Override
        public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
            return emptyList();
        }

        @Override
        public List<Class<? extends RestHandler>> getRestHandlers() {
            return emptyList();
        }
    }

    public static class MockLicenseService extends LicenseService {

        private final List<Licensee> licensees;

        @Inject
        public MockLicenseService(Settings settings, Environment environment,
                                  ResourceWatcherService resourceWatcherService, List<Licensee> licensees) {
            super(settings, null, null, environment, resourceWatcherService, licensees);
            this.licensees = licensees;
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
        public Licensee.Status licenseeStatus() {
            return null;
        }

        @Override
        public License getLicense() {
            return null;
        }

        @Override
        protected void doStart() {}

        @Override
        protected void doStop() {}
    }

    public static class InternalXPackPlugin extends XPackPlugin {
        public InternalXPackPlugin(Settings settings) throws IOException {
            super(settings);
            licensing = new MockLicensing();
        }
    }
}
