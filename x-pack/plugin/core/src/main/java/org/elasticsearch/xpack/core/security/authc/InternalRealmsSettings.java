/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.kerberos.KerberosRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class InternalRealmsSettings {
    private InternalRealmsSettings() {}

    /**
     * Provides the {@link Setting setting configuration} for each <em>internal</em> realm type.
     * This excludes the ReservedRealm, as it cannot be configured dynamically.
     */
    public static Set<Setting.AffixSetting<?>> getSettings() {
        Set<Setting.AffixSetting<?>> set = new HashSet<>();
        set.addAll(FileRealmSettings.getSettings());
        set.addAll(NativeRealmSettings.getSettings());
        set.addAll(LdapRealmSettings.getSettings(LdapRealmSettings.AD_TYPE));
        set.addAll(LdapRealmSettings.getSettings(LdapRealmSettings.LDAP_TYPE));
        set.addAll(PkiRealmSettings.getSettings());
        set.addAll(SamlRealmSettings.getSettings());
        set.addAll(KerberosRealmSettings.getSettings());
        set.addAll(OpenIdConnectRealmSettings.getSettings());
        set.addAll(JwtRealmSettings.getSettings());
        return Collections.unmodifiableSet(set);
    }
}
