/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.ldap.bind;

import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.GSSAPIBindRequest;
import com.unboundid.ldap.sdk.GSSAPIBindRequestProperties;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.SASLBindRequest;
import com.unboundid.ldap.sdk.SimpleBindRequest;

import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.BIND_DN;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.BIND_MODE;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.SASL_GSSAPI_DEBUG;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.SASL_GSSAPI_KEYTAB_PATH;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.SASL_GSSAPI_PRINCIPAL;
import static org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings.SECURE_BIND_PASSWORD;

/**
 * Supports building bind request from given configuration either a {@link SimpleBindRequest} or {@link SASLBindRequest}
 */
public final class BindRequestBuilder {
    private final RealmConfig realmConfig;
    private final Function<RealmConfig, String> extractBindDn;

    public BindRequestBuilder(final RealmConfig realmConfig, final Function<RealmConfig, String> extractBindDn) {
        this.realmConfig = realmConfig;
        this.extractBindDn = extractBindDn;
    }

    public BindRequest build() throws LDAPException {
        final BindRequest bindRequest;
        final String mode = realmConfig.getSetting(BIND_MODE);
        final byte[] bindPassword = bindPassword(realmConfig);
        switch (mode) {
        case "simple":
            bindRequest = buildSimpleBindRequest(extractBindDn.apply(realmConfig), bindPassword);
            break;
        case "sasl_gssapi":
            bindRequest = buildGSSAPIBindRequest(bindPassword);
            break;
        default:
            throw new IllegalArgumentException("unsupported bind request mode, valid modes ['simple', 'sasl_gssapi']");
        }
        return bindRequest;
    }

    private BindRequest buildGSSAPIBindRequest(final byte[] bindPassword) throws LDAPException {
        final BindRequest bindRequest;
        Path keytabPath = validateGSSAPISettings(bindPassword);

        final String principal = realmConfig.getSetting(SASL_GSSAPI_PRINCIPAL);
        final GSSAPIBindRequestProperties gssapiBindRequestProperties = new GSSAPIBindRequestProperties(principal, bindPassword);
        if (keytabPath != null) {
            gssapiBindRequestProperties.setUseKeyTab(true);
            gssapiBindRequestProperties.setKeyTabPath(keytabPath.toString());
        }
        if (realmConfig.hasSetting(SASL_GSSAPI_DEBUG)) {
            gssapiBindRequestProperties.setEnableGSSAPIDebugging(realmConfig.getSetting(SASL_GSSAPI_DEBUG));
        }
        gssapiBindRequestProperties.setRefreshKrb5Config(true);
        gssapiBindRequestProperties.setUseTicketCache(false);
        bindRequest = new GSSAPIBindRequest(gssapiBindRequestProperties);
        return bindRequest;
    }

    private Path validateGSSAPISettings(final byte[] bindPassword) {
        final String principal = realmConfig.getSetting(SASL_GSSAPI_PRINCIPAL);
        if (Strings.hasText(principal) == false) {
            throw new IllegalArgumentException(
                    "Principal setting [" + RealmSettings.getFullSettingKey(realmConfig, SASL_GSSAPI_PRINCIPAL) + "] must be configured");
        }

        final String bindDn = extractBindDn.apply(realmConfig);
        if (Strings.hasText(bindDn)) {
            throw new IllegalArgumentException(
                    "You cannot specify ["
                            + RealmSettings.getFullSettingKey(realmConfig, BIND_DN) + "] in 'sasl_gssapi' mode");
        }

        final String keyTabFile = realmConfig.getSetting(SASL_GSSAPI_KEYTAB_PATH);
        if (Strings.hasText(keyTabFile) == false && bindPassword == null) {
            throw new IllegalArgumentException(
                    "Either keytab [" + RealmSettings.getFullSettingKey(realmConfig, SASL_GSSAPI_KEYTAB_PATH) + "] or principal password "
                            + RealmSettings.getFullSettingKey(realmConfig, SECURE_BIND_PASSWORD) + " must be configured");
        }

        if (Strings.hasText(keyTabFile)) {
            if (bindPassword != null) {
                throw new IllegalArgumentException(
                        "You cannot specify both [" + RealmSettings.getFullSettingKey(realmConfig, SASL_GSSAPI_KEYTAB_PATH) + "] and (["
                                + RealmSettings.getFullSettingKey(realmConfig, LEGACY_BIND_PASSWORD) + "] or ["
                                + RealmSettings.getFullSettingKey(realmConfig, SECURE_BIND_PASSWORD) + "])");
            } else {
                final Path keytabPath = realmConfig.env().configFile().resolve(keyTabFile);
                if (keytabPath != null) {
                    if (Files.exists(keytabPath) == false) {
                        throw new IllegalArgumentException("configured key tab file [" + keytabPath + "] does not exist");
                    }
                    if (Files.isDirectory(keytabPath)) {
                        throw new IllegalArgumentException("configured key tab file [" + keytabPath + "] is a directory");
                    }
                    if (Files.isReadable(keytabPath) == false) {
                        throw new IllegalArgumentException("configured key tab file [" + keytabPath + "] must have read permission");
                    }
                }
                return keytabPath;
            }
        }
        return null;
    }

    private BindRequest buildSimpleBindRequest(final String bindDn, final byte[] bindPassword) {
        final BindRequest bindRequest;
        if (bindDn == null) {
            bindRequest = new SimpleBindRequest();
        } else {
            bindRequest = new SimpleBindRequest(bindDn, bindPassword);
        }
        return bindRequest;
    }

    private static byte[] bindPassword(RealmConfig config) {
        final byte[] bindPassword;
        if (config.hasSetting(LEGACY_BIND_PASSWORD)) {
            if (config.hasSetting(SECURE_BIND_PASSWORD)) {
                throw new IllegalArgumentException(
                        "You cannot specify both [" + RealmSettings.getFullSettingKey(config, LEGACY_BIND_PASSWORD) + "] and ["
                                + RealmSettings.getFullSettingKey(config, SECURE_BIND_PASSWORD) + "]");
            } else {
                bindPassword = CharArrays.toUtf8Bytes(config.getSetting(LEGACY_BIND_PASSWORD).getChars());
            }
        } else if (config.hasSetting(SECURE_BIND_PASSWORD)) {
            bindPassword = CharArrays.toUtf8Bytes(config.getSetting(SECURE_BIND_PASSWORD).getChars());
        } else {
            bindPassword = null;
        }
        return bindPassword;
    }
}
