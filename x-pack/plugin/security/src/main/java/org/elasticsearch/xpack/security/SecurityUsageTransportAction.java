/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.XPackSettings.API_KEY_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.FIPS_MODE_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.HTTP_SSL_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.TOKEN_SERVICE_ENABLED_SETTING;
import static org.elasticsearch.xpack.core.XPackSettings.TRANSPORT_SSL_ENABLED;

public class SecurityUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Settings settings;
    private final XPackLicenseState licenseState;
    private final Realms realms;
    private final CompositeRolesStore rolesStore;
    private final NativeRoleMappingStore roleMappingStore;
    private final IPFilter ipFilter;
    private final TokenService tokenService;
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
        this.tokenService = securityServices.tokenService;
        this.apiKeyService = securityServices.apiKeyService;
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        final boolean enabled = XPackSettings.SECURITY_ENABLED.get(settings);
        final Builder builder = new Builder(enabled, usage -> listener.onResponse(new XPackUsageFeatureResponse(usage)));

        builder.setSslUsage(sslUsage(settings));
        builder.setAuditUsage(auditUsage(settings));
        builder.setIpFilterUsage(ipFilterUsage(ipFilter));
        builder.setAnonymousUsage(Map.of("enabled", AnonymousUser.isAnonymousEnabled(settings)));
        builder.setFips140Usage(fips140Usage(settings));
        builder.setOperatorPrivilegesUsage(
            Map.of(
                "available",
                Security.OPERATOR_PRIVILEGES_FEATURE.checkWithoutTracking(licenseState),
                "enabled",
                OperatorPrivileges.OPERATOR_PRIVILEGES_ENABLED.get(settings)
            )
        );

        if (rolesStore == null || enabled == false) {
            builder.setRolesStoreUsage(Map.of());
        } else {
            rolesStore.usageStats(ActionListener.wrap(builder::setRolesStoreUsage, listener::onFailure));
        }

        if (roleMappingStore == null || enabled == false) {
            builder.setRoleMappingStoreUsage(Map.of("native", Map.of()));
        } else {
            roleMappingStore.usageStats(
                ActionListener.wrap(usage -> builder.setRoleMappingStoreUsage(Map.of("native", usage)), listener::onFailure)
            );
        }

        if (realms == null || enabled == false) {
            builder.setRealmsUsage(Map.of());
        } else {
            realms.usageStats(ActionListener.wrap(builder::setRealmsUsage, listener::onFailure));
        }

        if (tokenService == null || enabled == false) {
            builder.setTokenServiceUsage(Map.of());
        } else {
            tokenService.usageStats(ActionListener.wrap(builder::setTokenServiceUsage, listener::onFailure));
        }

        if (apiKeyService == null || enabled == false) {
            builder.setApiKeyServiceUsage(Map.of());
        } else {
            apiKeyService.usageStats(ActionListener.wrap(builder::setApiKeyServiceUsage, listener::onFailure));
        }
    }

    static Map<String, Object> sslUsage(Settings settings) {
        // If security has been explicitly disabled in the settings, then SSL is also explicitly disabled, and we don't want to report
        // these http/transport settings as they would be misleading (they could report `true` even though they were ignored)
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

    private static class Builder {

        private final CountDown countDown;
        private final boolean enabled;
        private final Consumer<SecurityFeatureSetUsage> callback;

        private final SetOnce<Map<String, Object>> realmsUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> rolesStoreUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> roleMappingStoreUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> sslUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> auditUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> ipFilterUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> anonymousUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> tokenServiceUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> apiKeyServiceUsage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> fips140Usage = new SetOnce<>();
        private final SetOnce<Map<String, Object>> operatorPrivilegesUsage = new SetOnce<>();

        Builder(boolean enabled, Consumer<SecurityFeatureSetUsage> callback) {
            this.enabled = enabled;
            this.callback = callback;
            this.countDown = new CountDown(11);
        }

        public void setRealmsUsage(Map<String, Object> realmsUsage) {
            this.realmsUsage.set(realmsUsage);
            countDown();
        }

        public void setRolesStoreUsage(Map<String, Object> rolesStoreUsage) {
            this.rolesStoreUsage.set(rolesStoreUsage);
            countDown();
        }

        public void setRoleMappingStoreUsage(Map<String, Object> roleMappingStoreUsage) {
            this.roleMappingStoreUsage.set(roleMappingStoreUsage);
            countDown();
        }

        public void setSslUsage(Map<String, Object> sslUsage) {
            this.sslUsage.set(sslUsage);
            countDown();
        }

        public void setAuditUsage(Map<String, Object> auditUsage) {
            this.auditUsage.set(auditUsage);
            countDown();
        }

        public void setIpFilterUsage(Map<String, Object> ipFilterUsage) {
            this.ipFilterUsage.set(ipFilterUsage);
            countDown();
        }

        public void setAnonymousUsage(Map<String, Object> anonymousUsage) {
            this.anonymousUsage.set(anonymousUsage);
            countDown();
        }

        public void setTokenServiceUsage(Map<String, Object> tokenServiceUsage) {
            this.tokenServiceUsage.set(tokenServiceUsage);
            countDown();
        }

        public void setApiKeyServiceUsage(Map<String, Object> apiKeyServiceUsage) {
            this.apiKeyServiceUsage.set(apiKeyServiceUsage);
            countDown();
        }

        public void setFips140Usage(Map<String, Object> fips140Usage) {
            this.fips140Usage.set(fips140Usage);
            countDown();
        }

        public void setOperatorPrivilegesUsage(Map<String, Object> operatorPrivilegesUsage) {
            this.operatorPrivilegesUsage.set(operatorPrivilegesUsage);
            countDown();
        }

        private void countDown() {
            if (this.countDown.countDown()) {
                callback.accept(
                    new SecurityFeatureSetUsage(
                        this.enabled,
                        this.realmsUsage.get(),
                        this.rolesStoreUsage.get(),
                        this.roleMappingStoreUsage.get(),
                        this.sslUsage.get(),
                        this.auditUsage.get(),
                        this.ipFilterUsage.get(),
                        this.anonymousUsage.get(),
                        this.tokenServiceUsage.get(),
                        this.apiKeyServiceUsage.get(),
                        this.fips140Usage.get(),
                        this.operatorPrivilegesUsage.get()
                    )
                );
            }
        }
    }
}
