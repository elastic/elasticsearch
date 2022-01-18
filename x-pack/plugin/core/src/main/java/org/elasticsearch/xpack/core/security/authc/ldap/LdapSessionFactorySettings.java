/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings.LDAP_TYPE;

public final class LdapSessionFactorySettings {

    public static final Setting.AffixSetting<List<String>> USER_DN_TEMPLATES_SETTING = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(LDAP_TYPE),
        "user_dn_templates",
        key -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Setting.Property.NodeScope)
    );

    public static Set<Setting.AffixSetting<?>> getSettings() {
        Set<Setting.AffixSetting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactorySettings.getSettings(LDAP_TYPE));
        settings.add(USER_DN_TEMPLATES_SETTING);
        return settings;
    }
}
