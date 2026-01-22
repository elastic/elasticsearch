/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SecurityInfoTransportActionTests extends ESTestCase {

    private Settings settings;
    private MockLicenseState licenseState;
    private Realms realms;
    private IPFilter ipFilter;
    private CompositeRolesStore rolesStore;
    private NativeRoleMappingStore roleMappingStore;
    private ProfileService profileService;
    private SecurityUsageServices securityServices;
    private ApiKeyService apiKeyService;

    @Before
    public void init() throws Exception {
        settings = Settings.builder().put("path.home", createTempDir()).build();
        licenseState = mock(MockLicenseState.class);
        realms = mock(Realms.class);
        ipFilter = mock(IPFilter.class);
        rolesStore = mock(CompositeRolesStore.class);
        roleMappingStore = mock(NativeRoleMappingStore.class);
        profileService = mock(ProfileService.class);
        apiKeyService = mock(ApiKeyService.class);
        securityServices = new SecurityUsageServices(realms, rolesStore, roleMappingStore, ipFilter, profileService, apiKeyService);
    }

    public void testAvailable() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        SecurityInfoTransportAction featureSet = new SecurityInfoTransportAction(transportService, mock(ActionFilters.class), settings);
        assertThat(featureSet.available(), is(true));
    }

    public void testEnabled() {
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        SecurityInfoTransportAction featureSet = new SecurityInfoTransportAction(transportService, mock(ActionFilters.class), settings);
        assertThat(featureSet.enabled(), is(true));

        Settings disabled = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), false).build();
        featureSet = new SecurityInfoTransportAction(transportService, mock(ActionFilters.class), disabled);
        assertThat(featureSet.enabled(), is(false));
    }

    @SuppressWarnings("rawtypes")
    public void testUsage() throws Exception {
        final boolean explicitlyDisabled = randomBoolean();
        final boolean enabled = explicitlyDisabled == false;
        final boolean operatorPrivilegesAvailable = randomBoolean();
        when(licenseState.isAllowed(Security.OPERATOR_PRIVILEGES_FEATURE)).thenReturn(operatorPrivilegesAvailable);
        final boolean remoteClusterServerAvailable = randomBoolean();
        when(licenseState.isAllowed(Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE)).thenReturn(remoteClusterServerAvailable);

        Settings.Builder settings = Settings.builder().put(this.settings);

        if (explicitlyDisabled) {
            settings.put("xpack.security.enabled", "false");
        }
        final boolean httpSSLEnabled = randomBoolean();
        settings.put("xpack.security.http.ssl.enabled", httpSSLEnabled);
        final boolean transportSSLEnabled = randomBoolean();
        settings.put("xpack.security.transport.ssl.enabled", transportSSLEnabled);

        // Remote cluster server requires security to be enabled
        final boolean remoteClusterServerEnabled = explicitlyDisabled ? false : randomBoolean();
        settings.put("remote_cluster_server.enabled", remoteClusterServerEnabled);
        final boolean remoteClusterServerSslEnabled = randomBoolean();
        settings.put("xpack.security.remote_cluster_server.ssl.enabled", remoteClusterServerSslEnabled);
        final boolean remoteClusterClientSslEnabled = randomBoolean();
        settings.put("xpack.security.remote_cluster_client.ssl.enabled", remoteClusterClientSslEnabled);

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
            apiKeyServiceEnabled = true; // this is the default
        }

        final boolean auditingEnabled = randomBoolean();
        settings.put(XPackSettings.AUDIT_ENABLED.getKey(), auditingEnabled);
        final boolean httpIpFilterEnabled = randomBoolean();
        final boolean transportIPFilterEnabled = randomBoolean();
        when(ipFilter.usageStats()).thenReturn(
            Map.of("http", Map.of("enabled", httpIpFilterEnabled), "transport", Map.of("enabled", transportIPFilterEnabled))
        );

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
        final boolean operatorPrivilegesEnabled = randomBoolean();
        if (operatorPrivilegesEnabled) {
            settings.put("xpack.security.operator_privileges.enabled", true);
        }

        final Map<String, Object> userProfileUsage = Map.of(
            "total",
            randomIntBetween(100, 200),
            "enabled",
            randomIntBetween(50, 99),
            "recent",
            randomIntBetween(1, 42)
        );
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final var listener = (ActionListener<Map<String, Object>>) invocation.getArguments()[0];
            listener.onResponse(userProfileUsage);
            return null;
        }).when(profileService).usageStats(anyActionListener());

        final int ccsKeys = randomIntBetween(0, 50);
        final int ccrKeys = randomIntBetween(0, 50);
        final int ccsCcrKeys = randomIntBetween(0, 50);
        final Map<String, Object> crossClusterApiKeyUsage = Map.of(
            "total",
            ccsKeys + ccrKeys + ccsCcrKeys,
            "ccs",
            ccsKeys,
            "ccr",
            ccrKeys,
            "ccs_ccr",
            ccsCcrKeys
        );
        doAnswer(invocation -> {
            final ActionListener<Map<String, Object>> listener = invocation.getArgument(0);
            listener.onResponse(apiKeyServiceEnabled ? crossClusterApiKeyUsage : Map.of());
            return null;
        }).when(apiKeyService).crossClusterApiKeyUsageStats(anyActionListener());

        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.localClusterStateOperation(null, null, null, future);
        SecurityFeatureSetUsage securityUsage = (SecurityFeatureSetUsage) future.get().getUsage();
        BytesStreamOutput out = new BytesStreamOutput();
        securityUsage.writeTo(out);
        XPackFeatureUsage serializedUsage = new SecurityFeatureSetUsage(out.bytes().streamInput());
        for (XPackFeatureUsage usage : Arrays.asList(securityUsage, serializedUsage)) {
            assertThat(usage, is(notNullValue()));
            assertThat(usage.name(), is(XPackField.SECURITY));
            assertThat(usage.enabled(), is(enabled));
            assertThat(usage.available(), is(true));
            XContentSource source = getXContentSource(usage);

            if (enabled) {
                for (int i = 0; i < 5; i++) {
                    assertThat(source.getValue("realms.type" + i + ".key1"), contains("value" + i));
                    assertThat(source.getValue("realms.type" + i + ".key2"), contains(i));
                    assertThat(source.getValue("realms.type" + i + ".key3"), contains(i % 2 == 0));
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

                // operator privileges
                assertThat(source.getValue("operator_privileges.available"), is(operatorPrivilegesAvailable));
                assertThat(source.getValue("operator_privileges.enabled"), is(operatorPrivilegesEnabled));

                // user profile
                assertThat(source.getValue("user_profile.total"), equalTo(userProfileUsage.get("total")));
                assertThat(source.getValue("user_profile.enabled"), equalTo(userProfileUsage.get("enabled")));
                assertThat(source.getValue("user_profile.recent"), equalTo(userProfileUsage.get("recent")));

                assertThat(source.getValue("ssl.http.enabled"), is(httpSSLEnabled));
                assertThat(source.getValue("ssl.transport.enabled"), is(transportSSLEnabled));
                if (remoteClusterServerEnabled) {
                    assertThat(source.getValue("ssl.remote_cluster_server.enabled"), is(remoteClusterServerSslEnabled));
                } else {
                    assertThat(source.getValue("ssl.remote_cluster_server.enabled"), nullValue());
                }
                assertThat(source.getValue("ssl.remote_cluster_client.enabled"), is(remoteClusterClientSslEnabled));
                assertThat(source.getValue("remote_cluster_server.available"), is(remoteClusterServerAvailable));
                assertThat(source.getValue("remote_cluster_server.enabled"), is(remoteClusterServerEnabled));
                if (apiKeyServiceEnabled) {
                    assertThat(source.getValue("remote_cluster_server.api_keys.total"), equalTo(crossClusterApiKeyUsage.get("total")));
                    assertThat(source.getValue("remote_cluster_server.api_keys.ccs"), equalTo(ccsKeys));
                    assertThat(source.getValue("remote_cluster_server.api_keys.ccr"), equalTo(ccrKeys));
                    assertThat(source.getValue("remote_cluster_server.api_keys.ccs_ccr"), equalTo(ccsCcrKeys));
                } else {
                    assertThat(source.getValue("remote_cluster_server.api_keys"), anEmptyMap());
                }
            } else {
                assertThat(source.getValue("ssl"), is(nullValue()));
                assertThat(source.getValue("realms"), is(nullValue()));
                assertThat(source.getValue("token_service"), is(nullValue()));
                assertThat(source.getValue("api_key_service"), is(nullValue()));
                assertThat(source.getValue("audit"), is(nullValue()));
                assertThat(source.getValue("anonymous"), is(nullValue()));
                assertThat(source.getValue("ipfilter"), is(nullValue()));
                assertThat(source.getValue("roles"), is(nullValue()));
                assertThat(source.getValue("operator_privileges"), is(nullValue()));
                assertThat(source.getValue("user_profile"), is(nullValue()));
                assertThat(source.getValue("remote_cluster_server"), is(nullValue()));
            }
        }
    }

    private XContentSource getXContentSource(XPackFeatureUsage usage) throws IOException {
        XContentSource source;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            source = new XContentSource(builder);
        }
        return source;
    }

    private void configureRealmsUsage(Map<String, Object> realmsUsageStats) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Map<String, Object>> listener = (ActionListener<Map<String, Object>>) invocationOnMock.getArguments()[0];
            listener.onResponse(realmsUsageStats);
            return Void.TYPE;
        }).when(realms).usageStats(anyActionListener());
    }

    private void configureRoleStoreUsage(boolean rolesStoreEnabled) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Map<String, Object>> listener = (ActionListener<Map<String, Object>>) invocationOnMock.getArguments()[0];
            if (rolesStoreEnabled) {
                listener.onResponse(Collections.singletonMap("count", 1));
            } else {
                listener.onResponse(Collections.emptyMap());
            }
            return Void.TYPE;
        }).when(rolesStore).usageStats(anyActionListener());
    }

    private void configureRoleMappingStoreUsage(boolean roleMappingStoreEnabled) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Map<String, Object>> listener = (ActionListener<Map<String, Object>>) invocationOnMock.getArguments()[0];
            if (roleMappingStoreEnabled) {
                final Map<String, Object> map = new HashMap<>();
                map.put("size", 12L);
                map.put("enabled", 10L);
                listener.onResponse(map);
            } else {
                listener.onResponse(Collections.emptyMap());
            }
            return Void.TYPE;
        }).when(roleMappingStore).usageStats(anyActionListener());
    }

    private SecurityUsageTransportAction newUsageAction(Settings settings) {
        ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
        return new SecurityUsageTransportAction(
            transportService,
            null,
            threadPool,
            mock(ActionFilters.class),
            settings,
            licenseState,
            securityServices
        );
    }
}
