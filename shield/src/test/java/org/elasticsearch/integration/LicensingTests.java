/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsIndices;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.Licensee;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.license.LicenseService;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.ShieldSettingsSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class LicensingTests extends ShieldIntegTestCase {

    public static final String ROLES =
            ShieldSettingsSource.DEFAULT_ROLE + ":\n" +
                    "  cluster: all\n" +
                    "  indices:\n" +
                    "    '*': manage\n" +
                    "    '/.*/': write\n" +
                    "    'test': read\n" +
                    "    'test1': read\n" +
                    "\n" +
                    "role_a:\n" +
                    "  indices:\n" +
                    "    'a': all\n" +
                    "\n" +
                    "role_b:\n" +
                    "  indices:\n" +
                    "    'b': all\n";

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
    protected Class<? extends Plugin> licensePluginClass() {
        return InternalLicensePlugin.class;
    }

    @Override
    protected String licensePluginName() {
        return InternalLicensePlugin.NAME;
    }

    @Test
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
            assertThat(ee.getHeader("es.license.expired.feature"), hasItem(LicenseService.FEATURE_NAME));
            assertThat(ee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            client.admin().cluster().prepareClusterStats().get();
            fail("expected an license expired exception when executing cluster stats action");
        } catch (ElasticsearchSecurityException ee) {
            // expected
            assertThat(ee.getHeader("es.license.expired.feature"), hasItem(LicenseService.FEATURE_NAME));
            assertThat(ee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            client.admin().cluster().prepareHealth().get();
            fail("expected an license expired exception when executing cluster health action");
        } catch (ElasticsearchSecurityException ee) {
            // expected
            assertThat(ee.getHeader("es.license.expired.feature"), hasItem(LicenseService.FEATURE_NAME));
            assertThat(ee.status(), is(RestStatus.UNAUTHORIZED));
        }

        try {
            client.admin().cluster().prepareNodesStats().get();
            fail("expected an license expired exception when executing cluster health action");
        } catch (ElasticsearchSecurityException ee) {
            // expected
            assertThat(ee.getHeader("es.license.expired.feature"), hasItem(LicenseService.FEATURE_NAME));
            assertThat(ee.status(), is(RestStatus.UNAUTHORIZED));
        }

        enableLicensing();

        IndicesStatsResponse indicesStatsResponse = client.admin().indices().prepareStats().get();
        assertNoFailures(indicesStatsResponse);

        ClusterStatsResponse clusterStatsNodeResponse = client.admin().cluster().prepareClusterStats().get();
        assertThat(clusterStatsNodeResponse, notNullValue());
        ClusterStatsIndices indices = clusterStatsNodeResponse.getIndicesStats();
        assertThat(indices, notNullValue());
        assertThat(indices.getIndexCount(), is(2));

        ClusterHealthResponse clusterIndexHealth = client.admin().cluster().prepareHealth().get();
        assertThat(clusterIndexHealth, notNullValue());

        NodesStatsResponse nodeStats = client.admin().cluster().prepareNodesStats().get();
        assertThat(nodeStats, notNullValue());
    }

    public static void disableLicensing() {
        for (InternalLicenseeRegistry service : internalCluster().getInstances(InternalLicenseeRegistry.class)) {
            service.disable();
        }
    }

    public static void enableLicensing() {
        for (InternalLicenseeRegistry service : internalCluster().getInstances(InternalLicenseeRegistry.class)) {
            service.enable();
        }
    }

    public static class InternalLicensePlugin extends Plugin {

        public static final String NAME = "internal-licensing";

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
            bind(InternalLicenseeRegistry.class).asEagerSingleton();
            bind(LicenseeRegistry.class).to(InternalLicenseeRegistry.class);
        }
    }

    public static class InternalLicenseeRegistry extends AbstractComponent implements LicenseeRegistry {

        private final List<Licensee> licensees = new ArrayList<>();

        static final License DUMMY_LICENSE = License.builder()
                .expiryDate(System.currentTimeMillis())
                .issueDate(System.currentTimeMillis())
                .issuedTo("LicensingTests")
                .issuer("test")
                .maxNodes(Integer.MAX_VALUE)
                .signature("_signature")
                .type("basic")
                .uid(String.valueOf(randomLong()) + System.identityHashCode(LicensingTests.class))
                .build();

        @Inject
        public InternalLicenseeRegistry(Settings settings) {
            super(settings);
            enable();
        }

        @Override
        public void register(Licensee licensee) {
            licensees.add(licensee);
            enable();
        }

        void enable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(DUMMY_LICENSE, LicenseState.ENABLED);
            }
        }

        void disable() {
            for (Licensee licensee : licensees) {
                licensee.onChange(DUMMY_LICENSE, LicenseState.DISABLED);
            }
        }
    }
}
