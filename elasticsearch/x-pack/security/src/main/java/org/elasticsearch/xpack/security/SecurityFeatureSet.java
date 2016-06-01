/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.security.authz.store.RolesStore;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;
import org.elasticsearch.xpack.security.transport.netty.SecurityNettyHttpServerTransport;
import org.elasticsearch.xpack.security.transport.netty.SecurityNettyTransport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class SecurityFeatureSet implements XPackFeatureSet {

    private final Settings settings;
    private final boolean enabled;
    private final SecurityLicenseState licenseState;
    @Nullable
    private final Realms realms;
    @Nullable
    private final RolesStore rolesStore;
    @Nullable
    private final IPFilter ipFilter;
    @Nullable
    private final AuditTrailService auditTrailService;
    @Nullable
    private final CryptoService cryptoService;

    @Inject
    public SecurityFeatureSet(Settings settings, @Nullable SecurityLicenseState licenseState,
                              @Nullable Realms realms, NamedWriteableRegistry namedWriteableRegistry, @Nullable RolesStore rolesStore,
                              @Nullable IPFilter ipFilter, @Nullable AuditTrailService auditTrailService,
                              @Nullable CryptoService cryptoService) {
        this.enabled = Security.enabled(settings);
        this.licenseState = licenseState;
        this.realms = realms;
        this.rolesStore = rolesStore;
        this.settings = settings;
        this.ipFilter = ipFilter;
        this.auditTrailService = auditTrailService;
        this.cryptoService = cryptoService;
        namedWriteableRegistry.register(Usage.class, Usage.writeableName(Security.NAME), Usage::new);
    }

    @Override
    public String name() {
        return Security.NAME;
    }

    @Override
    public String description() {
        return "Security for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.authenticationAndAuthorizationEnabled();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public XPackFeatureSet.Usage usage() {
        List<Map<String, Object>> enabledRealms = buildEnabledRealms(realms);
        Map<String, Object> rolesStoreUsage = rolesStoreUsage(rolesStore);
        Map<String, Object> sslUsage = sslUsage(settings);
        Map<String, Object> auditUsage = auditUsage(auditTrailService);
        Map<String, Object> ipFilterUsage = ipFilterUsage(ipFilter);
        boolean hasSystemKey = systemKeyUsage(cryptoService);
        return new Usage(available(), enabled(), enabledRealms, rolesStoreUsage, sslUsage, auditUsage, ipFilterUsage, hasSystemKey);
    }

    static List<Map<String, Object>> buildEnabledRealms(Realms realms) {
        if (realms == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> enabledRealms = new ArrayList<>();
        for (Realm realm : realms) {
            if (realm instanceof ReservedRealm) {
                continue; // we don't need usage of this one
            }
            Map<String, Object> stats = realm.usageStats();
            enabledRealms.add(stats);
        }
        return enabledRealms;
    }

    static Map<String, Object> rolesStoreUsage(@Nullable RolesStore rolesStore) {
        if (rolesStore == null) {
            return Collections.emptyMap();
        }
        return rolesStore.usageStats();
    }

    static Map<String, Object> sslUsage(Settings settings) {
        Map<String, Object> map = new HashMap<>(2);
        map.put("http", Collections.singletonMap("enabled", SecurityNettyHttpServerTransport.SSL_SETTING.get(settings)));
        map.put("transport", Collections.singletonMap("enabled", SecurityNettyTransport.SSL_SETTING.get(settings)));
        return map;
    }

    static Map<String, Object> auditUsage(@Nullable AuditTrailService auditTrailService) {
        if (auditTrailService == null) {
            return Collections.emptyMap();
        }
        return auditTrailService.usageStats();
    }

    static Map<String, Object> ipFilterUsage(@Nullable IPFilter ipFilter) {
        if (ipFilter == null) {
            return Collections.emptyMap();
        }
        return ipFilter.usageStats();
    }

    static boolean systemKeyUsage(CryptoService cryptoService) {
        // we can piggy back on the encryption enabled method as it is only enabled if there is a system key
        return cryptoService.encryptionEnabled();
    }

    static class Usage extends XPackFeatureSet.Usage {

        private static final String ENABLED_REALMS_XFIELD = "enabled_realms";
        private static final String ROLES_XFIELD = "roles";
        private static final String SSL_XFIELD = "ssl";
        private static final String AUDIT_XFIELD = "audit";
        private static final String IP_FILTER_XFIELD = "ipfilter";
        private static final String SYSTEM_KEY_XFIELD = "system_key";

        private List<Map<String, Object>> enabledRealms;
        private Map<String, Object> rolesStoreUsage;
        private Map<String, Object> sslUsage;
        private Map<String, Object> auditUsage;
        private Map<String, Object> ipFilterUsage;
        private boolean hasSystemKey;

        public Usage(StreamInput in) throws IOException {
            super(in);
            enabledRealms = in.readList(StreamInput::readMap);
            rolesStoreUsage = in.readMap();
            sslUsage = in.readMap();
            auditUsage = in.readMap();
            ipFilterUsage = in.readMap();
            hasSystemKey = in.readBoolean();
        }

        public Usage(boolean available, boolean enabled, List<Map<String, Object>> enabledRealms, Map<String, Object> rolesStoreUsage,
                     Map<String, Object> sslUsage, Map<String, Object> auditUsage, Map<String, Object> ipFilterUsage,
                     boolean hasSystemKey) {
            super(Security.NAME, available, enabled);
            this.enabledRealms = enabledRealms;
            this.rolesStoreUsage = rolesStoreUsage;
            this.sslUsage = sslUsage;
            this.auditUsage = auditUsage;
            this.ipFilterUsage = ipFilterUsage;
            this.hasSystemKey = hasSystemKey;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(enabledRealms.stream().map((m) -> (Writeable) o -> o.writeMap(m)).collect(Collectors.toList()));
            out.writeMap(rolesStoreUsage);
            out.writeMap(sslUsage);
            out.writeMap(auditUsage);
            out.writeMap(ipFilterUsage);
            out.writeBoolean(hasSystemKey);
        }

        @Override
        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerXContent(builder, params);
            if (enabled) {
                builder.field(ENABLED_REALMS_XFIELD, enabledRealms);
                builder.field(ROLES_XFIELD, rolesStoreUsage);
                builder.field(SSL_XFIELD, sslUsage);
                builder.field(AUDIT_XFIELD, auditUsage);
                builder.field(IP_FILTER_XFIELD, ipFilterUsage);
                builder.field(SYSTEM_KEY_XFIELD, hasSystemKey);
            }
        }
    }
}
