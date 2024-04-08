/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class SecurityFeatureSetUsage extends XPackFeatureSet.Usage {

    private static final String REALMS_XFIELD = "realms";
    private static final String ROLES_XFIELD = "roles";
    private static final String ROLE_MAPPING_XFIELD = "role_mapping";
    private static final String SSL_XFIELD = "ssl";
    private static final String TOKEN_SERVICE_XFIELD = "token_service";
    private static final String API_KEY_SERVICE_XFIELD = "api_key_service";
    private static final String AUDIT_XFIELD = "audit";
    private static final String IP_FILTER_XFIELD = "ipfilter";
    private static final String ANONYMOUS_XFIELD = "anonymous";
    private static final String FIPS_140_XFIELD = "fips_140";
    private static final String OPERATOR_PRIVILEGES_XFIELD = XPackField.OPERATOR_PRIVILEGES;
    private static final String DOMAINS_XFIELD = "domains";
    private static final String USER_PROFILE_XFIELD = "user_profile";
    private static final String REMOTE_CLUSTER_SERVER_XFIELD = "remote_cluster_server";

    private Map<String, Object> realmsUsage;
    private Map<String, Object> rolesStoreUsage;
    private Map<String, Object> sslUsage;
    private Map<String, Object> tokenServiceUsage;
    private Map<String, Object> apiKeyServiceUsage;
    private Map<String, Object> auditUsage;
    private Map<String, Object> ipFilterUsage;
    private Map<String, Object> anonymousUsage;
    private Map<String, Object> roleMappingStoreUsage;
    private Map<String, Object> fips140Usage;
    private Map<String, Object> operatorPrivilegesUsage;
    private Map<String, Object> domainsUsage;
    private Map<String, Object> userProfileUsage;
    private Map<String, Object> remoteClusterServerUsage;

    public SecurityFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        realmsUsage = in.readGenericMap();
        rolesStoreUsage = in.readGenericMap();
        sslUsage = in.readGenericMap();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_2_0)) {
            tokenServiceUsage = in.readGenericMap();
            apiKeyServiceUsage = in.readGenericMap();
        }
        auditUsage = in.readGenericMap();
        ipFilterUsage = in.readGenericMap();
        anonymousUsage = in.readGenericMap();
        roleMappingStoreUsage = in.readGenericMap();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_5_0)) {
            fips140Usage = in.readGenericMap();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_11_0)) {
            operatorPrivilegesUsage = in.readGenericMap();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            domainsUsage = in.readGenericMap();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_5_0)) {
            userProfileUsage = in.readGenericMap();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            remoteClusterServerUsage = in.readGenericMap();
        }
    }

    public SecurityFeatureSetUsage(
        boolean enabled,
        Map<String, Object> realmsUsage,
        Map<String, Object> rolesStoreUsage,
        Map<String, Object> roleMappingStoreUsage,
        Map<String, Object> sslUsage,
        Map<String, Object> auditUsage,
        Map<String, Object> ipFilterUsage,
        Map<String, Object> anonymousUsage,
        Map<String, Object> tokenServiceUsage,
        Map<String, Object> apiKeyServiceUsage,
        Map<String, Object> fips140Usage,
        Map<String, Object> operatorPrivilegesUsage,
        Map<String, Object> domainsUsage,
        Map<String, Object> userProfileUsage,
        Map<String, Object> remoteClusterServerUsage
    ) {
        super(XPackField.SECURITY, true, enabled);
        this.realmsUsage = realmsUsage;
        this.rolesStoreUsage = rolesStoreUsage;
        this.roleMappingStoreUsage = roleMappingStoreUsage;
        this.sslUsage = sslUsage;
        this.tokenServiceUsage = tokenServiceUsage;
        this.apiKeyServiceUsage = apiKeyServiceUsage;
        this.auditUsage = auditUsage;
        this.ipFilterUsage = ipFilterUsage;
        this.anonymousUsage = anonymousUsage;
        this.fips140Usage = fips140Usage;
        this.operatorPrivilegesUsage = operatorPrivilegesUsage;
        this.domainsUsage = domainsUsage;
        this.userProfileUsage = userProfileUsage;
        this.remoteClusterServerUsage = remoteClusterServerUsage;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_0_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(realmsUsage);
        out.writeGenericMap(rolesStoreUsage);
        out.writeGenericMap(sslUsage);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_2_0)) {
            out.writeGenericMap(tokenServiceUsage);
            out.writeGenericMap(apiKeyServiceUsage);
        }
        out.writeGenericMap(auditUsage);
        out.writeGenericMap(ipFilterUsage);
        out.writeGenericMap(anonymousUsage);
        out.writeGenericMap(roleMappingStoreUsage);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_5_0)) {
            out.writeGenericMap(fips140Usage);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_11_0)) {
            out.writeGenericMap(operatorPrivilegesUsage);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_2_0)) {
            out.writeGenericMap(domainsUsage);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_5_0)) {
            out.writeGenericMap(userProfileUsage);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
            out.writeGenericMap(remoteClusterServerUsage);
        }
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        if (enabled) {
            builder.field(REALMS_XFIELD, realmsUsage);
            builder.field(ROLES_XFIELD, rolesStoreUsage);
            builder.field(ROLE_MAPPING_XFIELD, roleMappingStoreUsage);
            builder.field(SSL_XFIELD, sslUsage);
            builder.field(TOKEN_SERVICE_XFIELD, tokenServiceUsage);
            builder.field(API_KEY_SERVICE_XFIELD, apiKeyServiceUsage);
            builder.field(AUDIT_XFIELD, auditUsage);
            builder.field(IP_FILTER_XFIELD, ipFilterUsage);
            builder.field(ANONYMOUS_XFIELD, anonymousUsage);
            builder.field(FIPS_140_XFIELD, fips140Usage);
            builder.field(OPERATOR_PRIVILEGES_XFIELD, operatorPrivilegesUsage);
            if (domainsUsage != null && false == domainsUsage.isEmpty()) {
                builder.field(DOMAINS_XFIELD, domainsUsage);
            }
            if (userProfileUsage != null && false == userProfileUsage.isEmpty()) {
                builder.field(USER_PROFILE_XFIELD, userProfileUsage);
            }
            if (remoteClusterServerUsage != null && false == remoteClusterServerUsage.isEmpty()) {
                builder.field(REMOTE_CLUSTER_SERVER_XFIELD, remoteClusterServerUsage);
            }
        } else if (sslUsage.isEmpty() == false) {
            // A trial (or basic) license can have SSL without security.
            // This is because security defaults to disabled on that license, but that dynamic-default does not disable SSL.
            builder.field(SSL_XFIELD, sslUsage);
        }
    }

    public Map<String, Object> getRealmsUsage() {
        return Collections.unmodifiableMap(realmsUsage);
    }

}
