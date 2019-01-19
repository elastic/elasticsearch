/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.junit.Before;

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

public class SecurityFeatureSetTests extends ESTestCase {

    private Settings settings;
    private XPackLicenseState licenseState;
    private Realms realms;
    private IPFilter ipFilter;
    private CompositeRolesStore rolesStore;
    private NativeRoleMappingStore roleMappingStore;

    @Before
    public void init() throws Exception {
        settings = Settings.builder().put("path.home", createTempDir()).build();
        licenseState = mock(XPackLicenseState.class);
        realms = mock(Realms.class);
        ipFilter = mock(IPFilter.class);
        rolesStore = mock(CompositeRolesStore.class);
        roleMappingStore = mock(NativeRoleMappingStore.class);
    }

    public void testAvailable() {
        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings, licenseState, realms,
                rolesStore, roleMappingStore, ipFilter);
        when(licenseState.isSecurityAvailable()).thenReturn(true);
        assertThat(featureSet.available(), is(true));

        when(licenseState.isSecurityAvailable()).thenReturn(false);
        assertThat(featureSet.available(), is(false));
    }

    public void testEnabled() {
        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings, licenseState, realms,
                rolesStore, roleMappingStore, ipFilter);
        assertThat(featureSet.enabled(), is(true));

        when(licenseState.isSecurityDisabledByTrialLicense()).thenReturn(true);
        featureSet = new SecurityFeatureSet(settings, licenseState, realms,
                rolesStore, roleMappingStore, ipFilter);
        assertThat(featureSet.enabled(), is(false));
    }

    public void testUsage() throws Exception {
        final boolean authcAuthzAvailable = randomBoolean();
        when(licenseState.isSecurityAvailable()).thenReturn(authcAuthzAvailable);

        Settings.Builder settings = Settings.builder().put(this.settings);

        boolean enabled = randomBoolean();
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), enabled);

        final boolean httpSSLEnabled = randomBoolean();
        settings.put("xpack.security.http.ssl.enabled", httpSSLEnabled);
        final boolean transportSSLEnabled = randomBoolean();
        settings.put("xpack.security.transport.ssl.enabled", transportSSLEnabled);
        final boolean auditingEnabled = randomBoolean();
        settings.put(XPackSettings.AUDIT_ENABLED.getKey(), auditingEnabled);
        final String[] auditOutputs = randomFrom(
                new String[] { "logfile" },
                new String[] { "index" },
                new String[] { "logfile", "index" }
        );
        settings.putList(Security.AUDIT_OUTPUTS_SETTING.getKey(), auditOutputs);
        final boolean httpIpFilterEnabled = randomBoolean();
        final boolean transportIPFilterEnabled = randomBoolean();
        when(ipFilter.usageStats())
                .thenReturn(MapBuilder.<String, Object>newMapBuilder()
                        .put("http", Collections.singletonMap("enabled", httpIpFilterEnabled))
                        .put("transport", Collections.singletonMap("enabled", transportIPFilterEnabled))
                        .map());


        final boolean rolesStoreEnabled = randomBoolean();
        doAnswer(invocationOnMock -> {
            ActionListener<Map<String, Object>> listener = (ActionListener<Map<String, Object>>) invocationOnMock.getArguments()[0];
            if (rolesStoreEnabled) {
                listener.onResponse(Collections.singletonMap("count", 1));
            } else {
                listener.onResponse(Collections.emptyMap());
            }
            return Void.TYPE;
        }).when(rolesStore).usageStats(any(ActionListener.class));

        final boolean roleMappingStoreEnabled = randomBoolean();
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

        Map<String, Object> realmsUsageStats = new HashMap<>();
        for (int i = 0; i < 5; i++) {
            Map<String, Object> realmUsage = new HashMap<>();
            realmsUsageStats.put("type" + i, realmUsage);
            realmUsage.put("key1", Arrays.asList("value" + i));
            realmUsage.put("key2", Arrays.asList(i));
            realmUsage.put("key3", Arrays.asList(i % 2 == 0));
        }
        doAnswer(invocationOnMock -> {
            ActionListener<Map<String, Object>> listener = (ActionListener) invocationOnMock.getArguments()[0];
            listener.onResponse(realmsUsageStats);
            return Void.TYPE;
        }).when(realms).usageStats(any(ActionListener.class));

        final boolean anonymousEnabled = randomBoolean();
        if (anonymousEnabled) {
            settings.put(AnonymousUser.ROLES_SETTING.getKey(), "foo");
        }

        SecurityFeatureSet featureSet = new SecurityFeatureSet(settings.build(), licenseState,
                realms, rolesStore, roleMappingStore, ipFilter);
        PlainActionFuture<XPackFeatureSet.Usage> future = new PlainActionFuture<>();
        featureSet.usage(future);
        XPackFeatureSet.Usage securityUsage = future.get();
        BytesStreamOutput out = new BytesStreamOutput();
        securityUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new SecurityFeatureSetUsage(out.bytes().streamInput());
        for (XPackFeatureSet.Usage usage : Arrays.asList(securityUsage, serializedUsage)) {
            assertThat(usage, is(notNullValue()));
            assertThat(usage.name(), is(XPackField.SECURITY));
            assertThat(usage.enabled(), is(enabled));
            assertThat(usage.available(), is(authcAuthzAvailable));
            XContentSource source;
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
                source = new XContentSource(builder);
            }

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

                // auditing
                assertThat(source.getValue("audit.enabled"), is(auditingEnabled));
                assertThat(source.getValue("audit.outputs"), contains(auditOutputs));

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
            } else {
                assertThat(source.getValue("realms"), is(nullValue()));
                assertThat(source.getValue("ssl"), is(nullValue()));
                assertThat(source.getValue("audit"), is(nullValue()));
                assertThat(source.getValue("anonymous"), is(nullValue()));
                assertThat(source.getValue("ipfilter"), is(nullValue()));
                assertThat(source.getValue("roles"), is(nullValue()));
            }
        }
    }
}
