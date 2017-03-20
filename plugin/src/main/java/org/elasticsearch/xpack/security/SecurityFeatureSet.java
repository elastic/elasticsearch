/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.user.AnonymousUser;

import static org.elasticsearch.xpack.XPackSettings.HTTP_SSL_ENABLED;

/**
 * Indicates whether the features of Security are currently in use
 */
public class SecurityFeatureSet implements XPackFeatureSet {

    private final Settings settings;
    private final boolean enabled;
    private final XPackLicenseState licenseState;
    @Nullable
    private final Realms realms;
    @Nullable
    private final CompositeRolesStore rolesStore;
    @Nullable
    private final IPFilter ipFilter;
    private final boolean systemKeyUsed;

    @Inject
    public SecurityFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState, @Nullable Realms realms,
                              @Nullable CompositeRolesStore rolesStore, @Nullable IPFilter ipFilter,
                              Environment environment) {
        this.enabled = XPackSettings.SECURITY_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.realms = realms;
        this.rolesStore = rolesStore;
        this.settings = settings;
        this.ipFilter = ipFilter;
        this.systemKeyUsed = enabled && Files.exists(CryptoService.resolveSystemKey(environment));
    }

    @Override
    public String name() {
        return XPackPlugin.SECURITY;
    }

    @Override
    public String description() {
        return "Security for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isAuthAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        Map<String, Object> realmsUsage = buildRealmsUsage(realms);
        Map<String, Object> sslUsage = sslUsage(settings);
        Map<String, Object> auditUsage = auditUsage(settings);
        Map<String, Object> ipFilterUsage = ipFilterUsage(ipFilter);
        Map<String, Object> systemKeyUsage = systemKeyUsage();
        Map<String, Object> anonymousUsage = Collections.singletonMap("enabled", AnonymousUser.isAnonymousEnabled(settings));
        final ActionListener<Map<String, Object>> rolesStoreUsageListener =
                ActionListener.wrap(rolesStoreUsage -> listener.onResponse(new Usage(available(), enabled(), realmsUsage, rolesStoreUsage,
                        sslUsage, auditUsage, ipFilterUsage, systemKeyUsage, anonymousUsage)),listener::onFailure);
        if (rolesStore == null) {
            rolesStoreUsageListener.onResponse(Collections.emptyMap());
        } else {
            rolesStore.usageStats(rolesStoreUsageListener);
        }
    }

    static Map<String, Object> buildRealmsUsage(Realms realms) {
        if (realms == null) {
            return Collections.emptyMap();
        }
        return realms.usageStats();
    }

    static Map<String, Object> sslUsage(Settings settings) {
        return Collections.singletonMap("http", Collections.singletonMap("enabled", HTTP_SSL_ENABLED.get(settings)));
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

    Map<String, Object> systemKeyUsage() {
        // we can piggy back on the encryption enabled method as it is only enabled if there is a system key
        return Collections.singletonMap("enabled", systemKeyUsed);
    }

    public static class Usage extends XPackFeatureSet.Usage {

        private static final String REALMS_XFIELD = "realms";
        private static final String ROLES_XFIELD = "roles";
        private static final String SSL_XFIELD = "ssl";
        private static final String AUDIT_XFIELD = "audit";
        private static final String IP_FILTER_XFIELD = "ipfilter";
        private static final String SYSTEM_KEY_XFIELD = "system_key";
        private static final String ANONYMOUS_XFIELD = "anonymous";

        private Map<String, Object> realmsUsage;
        private Map<String, Object> rolesStoreUsage;
        private Map<String, Object> sslUsage;
        private Map<String, Object> auditUsage;
        private Map<String, Object> ipFilterUsage;
        private Map<String, Object> systemKeyUsage;
        private Map<String, Object> anonymousUsage;

        public Usage(StreamInput in) throws IOException {
            super(in);
            realmsUsage = in.readMap();
            rolesStoreUsage = in.readMap();
            sslUsage = in.readMap();
            auditUsage = in.readMap();
            ipFilterUsage = in.readMap();
            systemKeyUsage = in.readMap();
            anonymousUsage = in.readMap();
        }

        public Usage(boolean available, boolean enabled, Map<String, Object> realmsUsage, Map<String, Object> rolesStoreUsage,
                     Map<String, Object> sslUsage, Map<String, Object> auditUsage, Map<String, Object> ipFilterUsage,
                     Map<String, Object> systemKeyUsage, Map<String, Object> anonymousUsage) {
            super(XPackPlugin.SECURITY, available, enabled);
            this.realmsUsage = realmsUsage;
            this.rolesStoreUsage = rolesStoreUsage;
            this.sslUsage = sslUsage;
            this.auditUsage = auditUsage;
            this.ipFilterUsage = ipFilterUsage;
            this.systemKeyUsage = systemKeyUsage;
            this.anonymousUsage = anonymousUsage;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(realmsUsage);
            out.writeMap(rolesStoreUsage);
            out.writeMap(sslUsage);
            out.writeMap(auditUsage);
            out.writeMap(ipFilterUsage);
            out.writeMap(systemKeyUsage);
            out.writeMap(anonymousUsage);
        }

        @Override
        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerXContent(builder, params);
            if (enabled) {
                builder.field(REALMS_XFIELD, realmsUsage);
                builder.field(ROLES_XFIELD, rolesStoreUsage);
                builder.field(SSL_XFIELD, sslUsage);
                builder.field(AUDIT_XFIELD, auditUsage);
                builder.field(IP_FILTER_XFIELD, ipFilterUsage);
                builder.field(SYSTEM_KEY_XFIELD, systemKeyUsage);
                builder.field(ANONYMOUS_XFIELD, anonymousUsage);
            }
        }
    }
}
