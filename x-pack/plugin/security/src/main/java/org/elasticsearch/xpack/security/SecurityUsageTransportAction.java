/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.XPackSettings.API_KEY_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.FIPS_MODE_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.TOKEN_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.TRANSPORT_SSL_ENABLED;

public class SecurityUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final boolean enabledInSettings;
    private final Settings settings;
    private final XPackLicenseState licenseState;
    private final Realms realms;
    private final CompositeRolesStore rolesStore;
    private final NativeRoleMappingStore roleMappingStore;
    private final IPFilter ipFilter;

    @Inject
    public SecurityUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        Settings settings, XPackLicenseState licenseState, SecurityUsageServices securityServices) {
        super(XPackUsageFeatureAction.SECURITY.name(), transportService, clusterService, threadPool,
              actionFilters, indexNameExpressionResolver);
        this.enabledInSettings = XPackSettings.SECURITY_ENABLED.get(settings);
        this.settings = settings;
        this.licenseState = licenseState;
        this.realms = securityServices.realms;
        this.rolesStore = securityServices.rolesStore;
        this.roleMappingStore = securityServices.roleMappingStore;
        this.ipFilter = securityServices.ipFilter;
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        Map<String, Object> sslUsage = sslUsage(settings);
        Map<String, Object> tokenServiceUsage = tokenServiceUsage(settings);
        Map<String, Object> apiKeyServiceUsage = apiKeyServiceUsage(settings);
        Map<String, Object> auditUsage = auditUsage(settings);
        Map<String, Object> ipFilterUsage = ipFilterUsage(ipFilter);
        Map<String, Object> anonymousUsage = singletonMap("enabled", AnonymousUser.isAnonymousEnabled(settings));
        Map<String, Object> fips140Usage = fips140Usage(settings);

        final AtomicReference<Map<String, Object>> rolesUsageRef = new AtomicReference<>();
        final AtomicReference<Map<String, Object>> roleMappingUsageRef = new AtomicReference<>();
        final AtomicReference<Map<String, Object>> realmsUsageRef = new AtomicReference<>();
        final CountDown countDown = new CountDown(3);
        final Runnable doCountDown = () -> {
            if (countDown.countDown()) {
                boolean enabled = enabledInSettings && licenseState.isSecurityDisabledByLicenseDefaults() == false;
                var usage = new SecurityFeatureSetUsage(licenseState.isSecurityAvailable(), enabled,
                        realmsUsageRef.get(), rolesUsageRef.get(), roleMappingUsageRef.get(), sslUsage, auditUsage,
                        ipFilterUsage, anonymousUsage, tokenServiceUsage, apiKeyServiceUsage, fips140Usage);
                listener.onResponse(new XPackUsageFeatureResponse(usage));
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
        // If security has been explicitly disabled in the settings, then SSL is also explicitly disabled, and we don't want to report
        //  these http/transport settings as they would be misleading (they could report `true` even though they were ignored)
        // But, if security has not been explicitly configured, but has defaulted to off due to the current license type,
        // then these SSL settings are still respected (that is SSL might be enabled, while the rest of security is disabled).
        if (XPackSettings.SECURITY_ENABLED.get(settings)) {
            Map<String, Object> map = new HashMap<>(2);
            map.put("http", singletonMap("enabled", HTTP_SSL_ENABLED.get(settings)));
            map.put("transport", singletonMap("enabled", TRANSPORT_SSL_ENABLED.get(settings)));
            return map;
        } else {
            return Collections.emptyMap();
        }
    }

    static Map<String, Object> tokenServiceUsage(Settings settings) {
        return singletonMap("enabled", TOKEN_SERVICE_ENABLED_SETTING.get(settings));
    }

    static Map<String, Object> apiKeyServiceUsage(Settings settings) {
        return singletonMap("enabled", API_KEY_SERVICE_ENABLED_SETTING.get(settings));
    }

    static Map<String, Object> auditUsage(Settings settings) {
        Map<String, Object> map = new HashMap<>(2);
        map.put("enabled", XPackSettings.AUDIT_ENABLED.get(settings));
        if (XPackSettings.AUDIT_ENABLED.get(settings)) {
            // the only available output type is "logfile", but the optputs=<list> is to keep compatibility with previous reporting format
            map.put("outputs", Arrays.asList(LoggingAuditTrail.NAME));
        }
        return map;
    }

    static Map<String, Object> ipFilterUsage(@Nullable IPFilter ipFilter) {
        if (ipFilter == null) {
            return IPFilter.DISABLED_USAGE_STATS;
        }
        return ipFilter.usageStats();
    }

    static Map<String, Object> fips140Usage(Settings settings) {
        return singletonMap("enabled", FIPS_MODE_ENABLED.get(settings));
    }
}
