/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.pki.PkiRealmSettings;
import org.elasticsearch.xpack.core.security.authc.saml.SamlRealmSettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class InternalRealmsSettings {
    private InternalRealmsSettings() {}

    /**
     * Provides the {@link Setting setting configuration} for each <em>internal</em> realm type.
     * This excludes the ReservedRealm, as it cannot be configured dynamically.
     * @return A map from <em>realm-type</em> to a collection of <code>Setting</code> objects.
     */
    public static Map<String, Set<Setting<?>>> getSettings() {
        Map<String, Set<Setting<?>>> map = new HashMap<>();
        map.put(FileRealmSettings.TYPE, FileRealmSettings.getSettings());
        map.put(NativeRealmSettings.TYPE, NativeRealmSettings.getSettings());
        map.put(LdapRealmSettings.AD_TYPE, LdapRealmSettings.getSettings(LdapRealmSettings.AD_TYPE));
        map.put(LdapRealmSettings.LDAP_TYPE, LdapRealmSettings.getSettings(LdapRealmSettings.LDAP_TYPE));
        map.put(PkiRealmSettings.TYPE, PkiRealmSettings.getSettings());
        map.put(SamlRealmSettings.TYPE, SamlRealmSettings.getSettings());
        return Collections.unmodifiableMap(map);
    }
}
