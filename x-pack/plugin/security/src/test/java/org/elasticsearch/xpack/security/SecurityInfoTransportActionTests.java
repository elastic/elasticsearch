/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityInfoTransportActionTests extends ESTestCase {

    private Settings settings;
    private XPackLicenseState licenseState;
    private Realms realms;
    private IPFilter ipFilter;
    private CompositeRolesStore rolesStore;
    private NativeRoleMappingStore roleMappingStore;
    private SecurityUsageServices securityServices;

    @Before
    public void init() throws Exception {
        settings = Settings.builder().put("path.home", createTempDir()).build();
        licenseState = mock(XPackLicenseState.class);
        realms = mock(Realms.class);
        ipFilter = mock(IPFilter.class);
        rolesStore = mock(CompositeRolesStore.class);
        roleMappingStore = mock(NativeRoleMappingStore.class);
        securityServices = new SecurityUsageServices(realms, rolesStore, roleMappingStore, ipFilter);
    }

    public void testAvailable() {
        SecurityInfoTransportAction featureSet = new SecurityInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        when(licenseState.isAllowed(XPackLicenseState.Feature.SECURITY)).thenReturn(true);
        assertThat(featureSet.available(), is(true));

        when(licenseState.isAllowed(XPackLicenseState.Feature.SECURITY)).thenReturn(false);
        assertThat(featureSet.available(), is(false));
    }

    public void testEnabled() {
        SecurityInfoTransportAction featureSet = new SecurityInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        assertThat(featureSet.enabled(), is(true));

        when(licenseState.isSecurityEnabled()).thenReturn(false);
        featureSet = new SecurityInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), licenseState);
        assertThat(featureSet.enabled(), is(false));
    }

    public void testUsage() throws Exception {
        final boolean authcAuthzAvailable = randomBoolean();
        final boolean explicitlyDisabled = randomBoolean();
        final boolean enabled = explicitlyDisabled == false && randomBoolean();
        when(licenseState.isAllowed(XPackLicenseState.Feature.SECURITY)).thenReturn(authcAuthzAvailable);
        when(licenseState.isSecurityEnabled()).thenReturn(enabled);

        Settings.Builder settings = Settings.builder().put(this.settings);

        if (explicitlyDisabled) {
            settings.put("xpack.security.enabled", "false");
        }
        final boolean httpSSLEnabled = randomBoolean();
        settings.put("xpack.security.http.ssl.enabled", httpSSLEnabled);
        final boolean transportSSLEnabled = randomBoolean();
        settings.put("xpack.security.transport.ssl.enabled", transportSSLEnabled);

        boolean configureEnabledFlagForTokenService = randomBoolean();
        final boolean tokenServiceEnabled;
        if (configureEnabledFlagForTokenService) {
            tokenServiceEnabled = randomBoolean();
            settings.put("xpack.security.authc.token.enabled", tokenServiceEnabled);
        } else {
            tokenServiceEnabled = httpSSLEnabled;
        }
        boolean configureEnabledFlagForApiKeyService = randomBoolean();
        final boolean apiKeyServiceEnabled;
        if (configureEnabledFlagForApiKeyService) {
            apiKeyServiceEnabled = randomBoolean();
            settings.put("xpack.security.authc.api_key.enabled", apiKeyServiceEnabled);
        } else {
            apiKeyServiceEnabled = httpSSLEnabled;
        }

        final boolean auditingEnabled = randomBoolean();
        settings.put(XPackSettings.AUDIT_ENABLED.getKey(), auditingEnabled);
        final boolean httpIpFilterEnabled = randomBoolean();
        final boolean transportIPFilterEnabled = randomBoolean();
        when(ipFilter.usageStats())
                .thenReturn(MapBuilder.<String, Object>newMapBuilder()
                        .put("http", Collections.singletonMap("enabled", httpIpFilterEnabled))
                        .put("transport", Collections.singletonMap("enabled", transportIPFilterEnabled))
                        .map());


        final boolean rolesStoreEnabled = randomBoolean();
        configureRoleStoreUsage(rolesStoreEnabled);

        final boolean roleMappingStoreEnabled = randomBoolean();
        configureRoleMappingStoreUsage(roleMappingStoreEnabled);

        Map<String, Object> realmsUsageStats = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Map<String, Object> realmUsage = new HashMap<>();
            realmsUsageStats.put("type" + i, realmUsage);
            realmUsage.put("key1", Arrays.asList("value" + i));
            realmUsage.put("key2", Arrays.asList(i));
            realmUsage.put("key3", Arrays.asList(i % 2 == 0));
        }
        configureRealmsUsage(realmsUsageStats);

        final boolean anonymousEnabled = randomBoolean();
        if (anonymousEnabled) {
            settings.put(AnonymousUser.ROLES_SETTING.getKey(), "foo");
        }

        final boolean fips140Enabled = randomBoolean();
        if (fips140Enabled) {
            settings.put("xpack.security.fips_mode.enabled", true);
        }

        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        SecurityFeatureSetUsage securityUsage = (SecurityFeatureSetUsage) future.get().getUsage();
        BytesStreamOutput out = new BytesStreamOutput();
        securityUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SecurityFeatureSetUsage(out.bytes().streamInput());
        for (XPackFeatureSet.Usage usage : Arrays.asList(securityUsage, serializedUsage)) {
            assertThat(usage, is(notNullValue()));
            assertThat(usage.name(), is(XPackField.SECURITY));
            assertThat(usage.enabled(), is(enabled));
            assertThat(usage.available(), is(authcAuthzAvailable));
            XContentSource source = getXContentSource(usage);

            if (enabled) {
                if (authcAuthzAvailable) {
                    for (int i = 0; i < 5; i++) {
                        assertThat(source.getValue("realms.type" + i + ".key1"), contains("value" + i));
                        assertThat(source.getValue("realms.type" + i + ".key2"), contains(i));
                        assertThat(source.getValue("realms.type" + i + ".key3"), contains(i % 2 == 0));
                    }
                } else {
                    assertThat(source.getValue("realms"), is(notNullValue()));
                }

                // check SSL
                assertThat(source.getValue("ssl.http.enabled"), is(httpSSLEnabled));
                assertThat(source.getValue("ssl.transport.enabled"), is(transportSSLEnabled));

                // check Token service
                assertThat(source.getValue("token_service.enabled"), is(tokenServiceEnabled));

                // check API Key service
                assertThat(source.getValue("api_key_service.enabled"), is(apiKeyServiceEnabled));

                // auditing
                assertThat(source.getValue("audit.enabled"), is(auditingEnabled));
                if (auditingEnabled) {
                    assertThat(source.getValue("audit.outputs"), contains(LoggingAuditTrail.NAME));
                } else {
                    assertThat(source.getValue("audit.outputs"), is(nullValue()));
                }

                // ip filter
                assertThat(source.getValue("ipfilter.http.enabled"), is(httpIpFilterEnabled));
                assertThat(source.getValue("ipfilter.transport.enabled"), is(transportIPFilterEnabled));

                // roles
                if (rolesStoreEnabled) {
                    assertThat(source.getValue("roles.count"), is(1));
                } else {
                    assertThat(((Map) source.getValue("roles")).isEmpty(), is(true));
                }

                // role-mapping
                if (roleMappingStoreEnabled) {
                    assertThat(source.getValue("role_mapping.native.size"), is(12));
                    assertThat(source.getValue("role_mapping.native.enabled"), is(10));
                } else {
                    final Map<String, Object> roleMapping = source.getValue("role_mapping.native");
                    assertThat(roleMapping.entrySet(), emptyIterable());
                }

                // anonymous
                assertThat(source.getValue("anonymous.enabled"), is(anonymousEnabled));

                // FIPS 140
                assertThat(source.getValue("fips_140.enabled"), is(fips140Enabled));
            } else {
                if (explicitlyDisabled) {
                    assertThat(source.getValue("ssl"), is(nullValue()));
                } else {
                    assertThat(source.getValue("ssl.http.enabled"), is(httpSSLEnabled));
                    assertThat(source.getValue("ssl.transport.enabled"), is(transportSSLEnabled));
                }
                assertThat(source.getValue("realms"), is(nullValue()));
                assertThat(source.getValue("token_service"), is(nullValue()));
                assertThat(source.getValue("api_key_service"), is(nullValue()));
                assertThat(source.getValue("audit"), is(nullValue()));
                assertThat(source.getValue("anonymous"), is(nullValue()));
                assertThat(source.getValue("ipfilter"), is(nullValue()));
                assertThat(source.getValue("roles"), is(nullValue()));
            }
        }
    }

    public void testUsageOnTrialLicenseWithSecurityDisabledByDefault() throws Exception {
        when(licenseState.isAllowed(XPackLicenseState.Feature.SECURITY)).thenReturn(true);
        when(licenseState.isSecurityEnabled()).thenReturn(false);

        Settings.Builder settings = Settings.builder().put(this.settings);

        final boolean httpSSLEnabled = randomBoolean();
        settings.put("xpack.security.http.ssl.enabled", httpSSLEnabled);
        final boolean transportSSLEnabled = randomBoolean();
        settings.put("xpack.security.transport.ssl.enabled", transportSSLEnabled);

        final boolean auditingEnabled = randomBoolean();
        settings.put(XPackSettings.AUDIT_ENABLED.getKey(), auditingEnabled);

        final boolean rolesStoreEnabled = randomBoolean();
        configureRoleStoreUsage(rolesStoreEnabled);

        final boolean roleMappingStoreEnabled = randomBoolean();
        configureRoleMappingStoreUsage(roleMappingStoreEnabled);

        configureRealmsUsage(Collections.emptyMap());

        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, null, future);
        SecurityFeatureSetUsage securityUsage = (SecurityFeatureSetUsage) future.get().getUsage();
        BytesStreamOutput out = new BytesStreamOutput();
        securityUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SecurityFeatureSetUsage(out.bytes().streamInput());
        for (XPackFeatureSet.Usage usage : Arrays.asList(securityUsage, serializedUsage)) {
            assertThat(usage, is(notNullValue()));
            assertThat(usage.name(), is(XPackField.SECURITY));
            assertThat(usage.enabled(), is(false));
            assertThat(usage.available(), is(true));
            XContentSource source = getXContentSource(usage);

            // check SSL : This is permitted even though security has been dynamically disabled by the trial license.
            assertThat(source.getValue("ssl"), is(notNullValue()));
            assertThat(source.getValue("ssl.http.enabled"), is(httpSSLEnabled));
            assertThat(source.getValue("ssl.transport.enabled"), is(transportSSLEnabled));

            // everything else is missing because security is disabled
            assertThat(source.getValue("realms"), is(nullValue()));
            assertThat(source.getValue("token_service"), is(nullValue()));
            assertThat(source.getValue("api_key_service"), is(nullValue()));
            assertThat(source.getValue("audit"), is(nullValue()));
            assertThat(source.getValue("anonymous"), is(nullValue()));
            assertThat(source.getValue("ipfilter"), is(nullValue()));
            assertThat(source.getValue("roles"), is(nullValue()));
        }
    }

    private XContentSource getXContentSource(XPackFeatureSet.Usage usage) throws IOException {
        XContentSource source;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            source = new XContentSource(builder);
        }
        return source;
    }

    private void configureRealmsUsage(Map<String, Object> realmsUsageStats) {
        doAnswer(invocationOnMock -> {
            ActionListener<Map<String, Object>> listener = (ActionListener) invocationOnMock.getArguments()[0];
            listener.onResponse(realmsUsageStats);
            return Void.TYPE;
        }).when(realms).usageStats(any(ActionListener.class));
    }

    private void configureRoleStoreUsage(boolean rolesStoreEnabled) {
        doAnswer(invocationOnMock -> {
            ActionListener<Map<String, Object>> listener = (ActionListener<Map<String, Object>>) invocationOnMock.getArguments()[0];
            if (rolesStoreEnabled) {
                listener.onResponse(Collections.singletonMap("count", 1));
            } else {
                listener.onResponse(Collections.emptyMap());
            }
            return Void.TYPE;
        }).when(rolesStore).usageStats(any(ActionListener.class));
    }

    private void configureRoleMappingStoreUsage(boolean roleMappingStoreEnabled) {
        doAnswer(invocationOnMock -> {
            ActionListener<Map<String, Object>> listener = (ActionListener) invocationOnMock.getArguments()[0];
            if (roleMappingStoreEnabled) {
                final Map<String, Object> map = new HashMap<>();
                map.put("size", 12L);
                map.put("enabled", 10L);
                listener.onResponse(map);
            } else {
                listener.onResponse(Collections.emptyMap());
            }
            return Void.TYPE;
        }).when(roleMappingStore).usageStats(any(ActionListener.class));
    }

    private SecurityUsageTransportAction newUsageAction(Settings settings) {
        return new SecurityUsageTransportAction(mock(TransportService.class),null,
            null, mock(ActionFilters.class),null,
            settings, licenseState, securityServices);
    }
}
