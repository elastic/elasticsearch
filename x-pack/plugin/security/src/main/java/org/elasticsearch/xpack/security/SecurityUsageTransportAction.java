/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.security.SecurityFeatureSetUsage;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.audit.logfile.LoggingAuditTrail;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;
import org.elasticsearch.xpack.security.profile.ProfileService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.XPackSettings.API_KEY_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.FIPS_MODE_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.TOKEN_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.TRANSPORT_SSL_ENABLED;
import static org.elasticsearch.xpack.security.Security.ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE;

public class SecurityUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Settings settings;
    private final XPackLicenseState licenseState;
    private final Realms realms;
    private final CompositeRolesStore rolesStore;
    private final NativeRoleMappingStore roleMappingStore;
    private final IPFilter ipFilter;
    private final ProfileService profileService;
    private final ApiKeyService apiKeyService;

    @Inject
    public SecurityUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings,
        XPackLicenseState licenseState,
        SecurityUsageServices securityServices
    ) {
        super(
            XPackUsageFeatureAction.SECURITY.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.settings = settings;
        this.licenseState = licenseState;
        this.realms = securityServices.realms;
        this.rolesStore = securityServices.rolesStore;
        this.roleMappingStore = securityServices.roleMappingStore;
        this.ipFilter = securityServices.ipFilter;
        this.profileService = securityServices.profileService;
        this.apiKeyService = securityServices.apiKeyService;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        Map<String, Object> sslUsage = sslUsage(settings);
        Map<String, Object> tokenServiceUsage = tokenServiceUsage(settings);
        Map<String, Object> apiKeyServiceUsage = apiKeyServiceUsage(settings);
        Map<String, Object> auditUsage = auditUsage(settings);
        Map<String, Object> ipFilterUsage = ipFilterUsage(ipFilter);
        Map<String, Object> anonymousUsage = singletonMap("enabled", AnonymousUser.isAnonymousEnabled(settings));
        Map<String, Object> fips140Usage = fips140Usage(settings);
        Map<String, Object> operatorPrivilegesUsage = Map.of(
            "available",
            Security.OPERATOR_PRIVILEGES_FEATURE.checkWithoutTracking(licenseState),
            "enabled",
            OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED.get(settings)
        );

        final AtomicReference<Map<String, Object>> rolesUsageRef = new AtomicReference<>(Map.of());
        final AtomicReference<Map<String, Object>> roleMappingUsageRef = new AtomicReference<>(Map.of());
        final AtomicReference<Map<String, Object>> realmsUsageRef = new AtomicReference<>(Map.of());
        final AtomicReference<Map<String, Object>> domainsUsageRef = new AtomicReference<>(Map.of());
        final AtomicReference<Map<String, Object>> userProfileUsageRef = new AtomicReference<>(Map.of());
        final AtomicReference<Map<String, Object>> remoteClusterServerUsageRef = new AtomicReference<>(Map.of());

        final boolean enabled = XPackSettings.SECURITY_ENABLED.get(settings);

        try (
            var listeners = new RefCountingListener(
                listener.map(
                    ignored -> new XPackUsageFeatureResponse(
                        new SecurityFeatureSetUsage(
                            enabled,
                            realmsUsageRef.get(),
                            rolesUsageRef.get(),
                            roleMappingUsageRef.get(),
                            sslUsage,
                            auditUsage,
                            ipFilterUsage,
                            anonymousUsage,
                            tokenServiceUsage,
                            apiKeyServiceUsage,
                            fips140Usage,
                            operatorPrivilegesUsage,
                            domainsUsageRef.get(),
                            userProfileUsageRef.get(),
                            remoteClusterServerUsageRef.get()
                        )
                    )
                )
            )
        ) {
            if (enabled == false) {
                return;
            }
            if (rolesStore != null) {
                rolesStore.usageStats(listeners.acquire(rolesUsageRef::set));
            }
            if (roleMappingStore != null) {
                roleMappingStore.usageStats(
                    listeners.acquire(nativeRoleMappingStoreUsage -> roleMappingUsageRef.set(Map.of("native", nativeRoleMappingStoreUsage)))
                );
            }
            if (realms != null) {
                domainsUsageRef.set(realms.domainUsageStats());
                realms.usageStats(listeners.acquire(realmsUsageRef::set));
            }
            if (profileService != null) {
                profileService.usageStats(listeners.acquire(userProfileUsageRef::set));
            }
            if (apiKeyService != null) {
                apiKeyService.crossClusterApiKeyUsageStats(
                    listeners.acquire(
                        usage -> remoteClusterServerUsageRef.set(
                            Map.of(
                                "available",
                                ADVANCED_REMOTE_CLUSTER_SECURITY_FEATURE.checkWithoutTracking(licenseState),
                                "enabled",
                                RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.get(settings),
                                "api_keys",
                                usage
                            )
                        )
                    )
                );
            }
        }
    }

    static Map<String, Object> sslUsage(Settings settings) {
        // If security has been explicitly disabled in the settings, then SSL is also explicitly disabled, and we don't want to report
        // these http/transport settings as they would be misleading (they could report `true` even though they were ignored)
        if (XPackSettings.SECURITY_ENABLED.get(settings)) {
            Map<String, Object> map = Maps.newMapWithExpectedSize(2);
            map.put("http", singletonMap("enabled", HTTP_SSL_ENABLED.get(settings)));
            map.put("transport", singletonMap("enabled", TRANSPORT_SSL_ENABLED.get(settings)));
            if (RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.get(settings)) {
                map.put("remote_cluster_server", singletonMap("enabled", REMOTE_CLUSTER_SERVER_SSL_ENABLED.get(settings)));
            }
            map.put("remote_cluster_client", singletonMap("enabled", REMOTE_CLUSTER_CLIENT_SSL_ENABLED.get(settings)));
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
        Map<String, Object> map = Maps.newMapWithExpectedSize(2);
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
