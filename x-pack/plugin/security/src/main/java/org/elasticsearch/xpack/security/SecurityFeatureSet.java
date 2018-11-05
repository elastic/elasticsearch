/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.TRANSPORT_SSL_ENABLED;

/**
 * Indicates whether the features of Security are currently in use
 */
public class SecurityFeatureSet implements XPackFeatureSet {

    private final Settings settings;
    private final XPackLicenseState licenseState;
    @Nullable
    private final Realms realms;
    @Nullable
    private final CompositeRolesStore rolesStore;
    @Nullable
    private final NativeRoleMappingStore roleMappingStore;
    @Nullable
    private final IPFilter ipFilter;

    @Inject
    public SecurityFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState,
                              @Nullable Realms realms, @Nullable CompositeRolesStore rolesStore,
                              @Nullable NativeRoleMappingStore roleMappingStore,
                              @Nullable IPFilter ipFilter) {
        this.licenseState = licenseState;
        this.realms = realms;
        this.rolesStore = rolesStore;
        this.roleMappingStore = roleMappingStore;
        this.settings = settings;
        this.ipFilter = ipFilter;
    }

    @Override
    public String name() {
        return XPackField.SECURITY;
    }

    @Override
    public String description() {
        return "Security for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isSecurityAvailable();
    }

    @Override
    public boolean enabled() {
        if (licenseState != null) {
            return XPackSettings.SECURITY_ENABLED.get(settings) &&
                licenseState.isSecurityDisabledByTrialLicense() == false;
        }
        return false;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        Map<String, Object> sslUsage = sslUsage(settings);
        Map<String, Object> auditUsage = auditUsage(settings);
        Map<String, Object> ipFilterUsage = ipFilterUsage(ipFilter);
        Map<String, Object> anonymousUsage = singletonMap("enabled", AnonymousUser.isAnonymousEnabled(settings));

        final AtomicReference<Map<String, Object>> rolesUsageRef = new AtomicReference<>();
        final AtomicReference<Map<String, Object>> roleMappingUsageRef = new AtomicReference<>();
        final AtomicReference<Map<String, Object>> realmsUsageRef = new AtomicReference<>();
        final CountDown countDown = new CountDown(3);
        final Runnable doCountDown = () -> {
            if (countDown.countDown()) {
                listener.onResponse(new SecurityFeatureSetUsage(available(), enabled(), realmsUsageRef.get(),
                        rolesUsageRef.get(), roleMappingUsageRef.get(),
                        sslUsage, auditUsage, ipFilterUsage, anonymousUsage));
            }
        };

        final ActionListener<Map<String, Object>> rolesStoreUsageListener =
                ActionListener.wrap(rolesStoreUsage -> {
                    rolesUsageRef.set(rolesStoreUsage);
                    doCountDown.run();
                }, listener::onFailure);

        final ActionListener<Map<String, Object>> roleMappingStoreUsageListener =
                ActionListener.wrap(nativeRoleMappingStoreUsage -> {
                    Map<String, Object> usage = singletonMap("native", nativeRoleMappingStoreUsage);
                    roleMappingUsageRef.set(usage);
                    doCountDown.run();
                }, listener::onFailure);

        final ActionListener<Map<String, Object>> realmsUsageListener =
            ActionListener.wrap(realmsUsage -> {
                realmsUsageRef.set(realmsUsage);
                doCountDown.run();
            }, listener::onFailure);

        if (rolesStore == null) {
            rolesStoreUsageListener.onResponse(Collections.emptyMap());
        } else {
            rolesStore.usageStats(rolesStoreUsageListener);
        }
        if (roleMappingStore == null) {
            roleMappingStoreUsageListener.onResponse(Collections.emptyMap());
        } else {
            roleMappingStore.usageStats(roleMappingStoreUsageListener);
        }
        if (realms == null) {
            realmsUsageListener.onResponse(Collections.emptyMap());
        } else {
            realms.usageStats(realmsUsageListener);
        }
    }

    static Map<String, Object> sslUsage(Settings settings) {
        Map<String, Object> map = new HashMap<>(2);
        map.put("http", singletonMap("enabled", HTTP_SSL_ENABLED.get(settings)));
        map.put("transport", singletonMap("enabled", TRANSPORT_SSL_ENABLED.get(settings)));
        return map;
    }

    static Map<String, Object> auditUsage(Settings settings) {
        Map<String, Object> map = new HashMap<>(2);
        map.put("enabled", XPackSettings.AUDIT_ENABLED.get(settings));
        map.put("outputs", Security.AUDIT_OUTPUTS_SETTING.get(settings));
        return map;
    }

    static Map<String, Object> ipFilterUsage(@Nullable IPFilter ipFilter) {
        if (ipFilter == null) {
            return IPFilter.DISABLED_USAGE_STATS;
        }
        return ipFilter.usageStats();
    }

}
