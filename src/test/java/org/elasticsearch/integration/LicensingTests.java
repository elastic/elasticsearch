/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.plugin.core.LicenseExpiredException;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.license.LicenseService;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope = SUITE)
public class LicensingTests extends ShieldIntegrationTest {

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
    public void testEnableDisbleBehaviour() throws Exception {
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
            client.prepareSearch().setQuery(matchAllQuery()).get();
            fail("expected an license expired exception when running a search with disabled license");
        } catch (LicenseExpiredException lee) {
            // expected
            assertThat(lee.feature(), equalTo(LicenseService.FEATURE_NAME));
        }

        try {
            client.prepareGet("test1", "type", indexResponse.getId()).get();
            fail("expected an license expired exception when running a get with disabled license");
        } catch (LicenseExpiredException lee) {
            // expected
            assertThat(lee.feature(), equalTo(LicenseService.FEATURE_NAME));
        }

        enableLicensing();

        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        GetResponse getResponse = client.prepareGet("test1", "type", indexResponse.getId()).get();
        assertThat(getResponse.getId(), equalTo(indexResponse.getId()));

        enableLicensing();
        indexResponse = index("test", "type", jsonBuilder()
                .startObject()
                .field("name", "value2")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        disableLicensing();

        indexResponse = index("test", "type", jsonBuilder()
                .startObject()
                .field("name", "value3")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));
    }

    private void disableLicensing() {
        for (InternalLicensesClientService service : internalCluster().getInstances(InternalLicensesClientService.class)) {
            service.disable();
        }
    }

    private void enableLicensing() {
        for (InternalLicensesClientService service : internalCluster().getInstances(InternalLicensesClientService.class)) {
            service.enable();
        }
    }

    public static class InternalLicensePlugin extends AbstractPlugin {

        static final String NAME = "internal-licensing";

        @Override
        public String name() {
            return NAME;
        }

        @Override
        public String description() {
            return name();
        }

        @Override
        public Collection<Class<? extends Module>> modules() {
            return ImmutableSet.<Class<? extends Module>>of(InternalLicenseModule.class);
        }
    }

    public static class InternalLicenseModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(InternalLicensesClientService.class).asEagerSingleton();
            bind(LicensesClientService.class).to(InternalLicensesClientService.class);
        }
    }

    public static class InternalLicensesClientService extends AbstractComponent implements LicensesClientService {

        private final List<Listener> listeners = new ArrayList<>();

        @Inject
        public InternalLicensesClientService(Settings settings) {
            super(settings);
            enable();
        }

        @Override
        public void register(String feature, LicensesService.TrialLicenseOptions trialLicenseOptions, Listener listener) {
            listeners.add(listener);
            enable();
        }

        void enable() {
            for (Listener listener : listeners) {
                listener.onEnabled();
            }
        }

        void disable() {
            for (Listener listener : listeners) {
                listener.onDisabled();
            }
        }
    }
}
