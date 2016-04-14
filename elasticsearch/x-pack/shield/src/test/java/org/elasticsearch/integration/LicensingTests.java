/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsIndices;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.Licensing;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class LicensingTests extends ShieldIntegTestCase {
    public static final String ROLES =
            ShieldSettingsSource.DEFAULT_ROLE + ":\n" +
                    "  cluster: [ all ]\n" +
                    "  indices:\n" +
                    "    - names: '*'\n" +
                    "      privileges: [manage]\n" +
                    "    - names: '/.*/'\n" +
                    "      privileges: [write]\n" +
                    "    - names: 'test'\n" +
                    "      privileges: [read]\n" +
                    "    - names: 'test1'\n" +
                    "      privileges: [read]\n" +
                    "\n" +
                    "role_a:\n" +
                    "  indices:\n" +
                    "    - names: 'a'\n" +
                    "      privileges: [all]\n" +
                    "\n" +
                    "role_b:\n" +
                    "  indices:\n" +
                    "    - names: 'b'\n" +
                    "      privileges: [all]\n";

    public static final String USERS =
            ShieldSettingsSource.CONFIG_STANDARD_USER +
                    "user_a:{plain}passwd\n" +
                    "user_b:{plain}passwd\n";

    public static final String USERS_ROLES =
            ShieldSettingsSource.CONFIG_STANDARD_USER_ROLES +
                    "role_a:user_a,user_b\n" +
                    "role_b:user_b\n";

    @Override
    protected String configRoles() {
        return ROLES;
    }

    @Override
    protected String configUsers() {
        return USERS;
    }

    @Override
    protected String configUsersRoles() {
        return USERS_ROLES;
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .build();
    }

    @Override
    protected Class<? extends XPackPlugin> xpackPluginClass() {
        return InternalXPackPlugin.class;
    }

    @After
    public void resetLicensing() {
        enableLicensing();
    }

    public void testEnableDisableBehaviour() throws Exception {
        IndexResponse indexResponse = index("test", "type", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));


        indexResponse = index("test1", "type", jsonBuilder()
                .startObject()
                .field("name", "value1")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        disableLicensing();

        try {
            client.admin().indices().prepareStats().get();
            fail("expected an license expired exception when executing an index stats action");
        } catch (ElasticsearchSecurityException ee) {
            // expected
            assertThat(ee.getHeader("es.license.expired.feature"), hasItem(Security.NAME));
            assertThat(ee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            client.admin().cluster().prepareClusterStats().get();
            fail("expected an license expired exception when executing cluster stats action");
        } catch (ElasticsearchSecurityException ee) {
            // expected
            assertThat(ee.getHeader("es.license.expired.feature"), hasItem(Security.NAME));
            assertThat(ee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            client.admin().cluster().prepareHealth().get();
            fail("expected an license expired exception when executing cluster health action");
        } catch (ElasticsearchSecurityException ee) {
            // expected
            assertThat(ee.getHeader("es.license.expired.feature"), hasItem(Security.NAME));
            assertThat(ee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            client.admin().cluster().prepareNodesStats().get();
            fail("expected an license expired exception when executing cluster health action");
        } catch (ElasticsearchSecurityException ee) {
            // expected
            assertThat(ee.getHeader("es.license.expired.feature"), hasItem(Security.NAME));
            assertThat(ee.status(), is(RestStatus.UNAUTHORIZED));
        }

        enableLicensing(randomFrom(OperationMode.values()));

        IndicesStatsResponse indicesStatsResponse = client.admin().indices().prepareStats().get();
        assertNoFailures(indicesStatsResponse);

        ClusterStatsResponse clusterStatsNodeResponse = client.admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsNodeResponse, notNullValue());
        ClusterStatsIndices indices = clusterStatsNodeResponse.getIndicesStats();
        assertThat(indices, notNullValue());
        assertThat(indices.getIndexCount(), greaterThanOrEqualTo(2));

        ClusterHealthResponse clusterIndexHealth = client.admin().cluster().prepareHealth().get();
        assertThat(clusterIndexHealth, notNullValue());

        NodesStatsResponse nodeStats = client.admin().cluster().prepareNodesStats().get();
        assertThat(nodeStats, notNullValue());
    }

    public void testRestAuthenticationByLicenseType() throws Exception {
        // the default of the licensing tests is basic
        assertThat(httpClient().path("/").execute().getStatusCode(), is(200));

        // generate a new license with a mode that enables auth
        OperationMode mode = randomFrom(OperationMode.GOLD, OperationMode.TRIAL, OperationMode.PLATINUM, OperationMode.STANDARD);
        enableLicensing(mode);
        assertThat(httpClient().path("/").execute().getStatusCode(), is(401));
    }

    public void testTransportClientAuthenticationByLicenseType() throws Exception {
        Settings.Builder builder = Settings.builder()
            .put(internalCluster().transportClient().settings());
        // remove user info
        builder.remove(Security.USER_SETTING.getKey());
        builder.remove(ThreadContext.PREFIX + "." + UsernamePasswordToken.BASIC_AUTH_HEADER);

        // basic has no auth
        try (TransportClient client = TransportClient.builder().settings(builder).addPlugin(XPackPlugin.class).build()) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            assertGreenClusterState(client);
        }

        // enable a license that enables security
        OperationMode mode = randomFrom(OperationMode.GOLD, OperationMode.TRIAL, OperationMode.PLATINUM, OperationMode.STANDARD);
        enableLicensing(mode);

        try (TransportClient client = TransportClient.builder().settings(builder).addPlugin(XPackPlugin.class).build()) {
            client.addTransportAddress(internalCluster().getDataNodeInstance(Transport.class).boundAddress().publishAddress());
            client.admin().cluster().prepareHealth().get();
            fail("should not have been able to connect to a node!");
        } catch (NoNodeAvailableException e) {
            // expected
        }
    }

    public static void disableLicensing() {
        disableLicensing(OperationMode.BASIC);
    }

    public static void disableLicensing(OperationMode operationMode) {
        for (InternalLicenseeRegistry service : internalCluster().getInstances(InternalLicenseeRegistry.class)) {
            service.disable(operationMode);
        }
    }

    public static void enableLicensing() {
        enableLicensing(OperationMode.BASIC);
    }

    public static void enableLicensing(OperationMode operationMode) {
        for (InternalLicenseeRegistry service : internalCluster().getInstances(InternalLicenseeRegistry.class)) {
            service.enable(operationMode);
        }
    }

    public static class InternalLicensing extends Licensing {

        @Override
        public Collection<Module> nodeModules() {
            return Collections.<Module>singletonList(new InternalLicenseModule());
        }

        public InternalLicensing() {
            super(Settings.EMPTY);
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

    public static class InternalLicenseModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(InternalLicenseeRegistry.class).asEagerSingleton();
            bind(LicenseeRegistry.class).to(InternalLicenseeRegistry.class);
        }
    }

    public static class InternalLicenseeRegistry extends AbstractComponent implements LicenseeRegistry {

        private final List<Licensee> licensees = new ArrayList<>();

        @Inject
        public InternalLicenseeRegistry(Settings settings) {
            super(settings);
            enable(OperationMode.BASIC);
        }

        @Override
        public void register(Licensee licensee) {
            licensees.add(licensee);
            enable(OperationMode.BASIC);
        }

        void enable(OperationMode operationMode) {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(operationMode, LicenseState.ENABLED));
            }
        }

        void disable(OperationMode operationMode) {
            for (Licensee licensee : licensees) {
                licensee.onChange(new Licensee.Status(operationMode, LicenseState.DISABLED));
            }
        }
    }
}
