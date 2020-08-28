/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings.LDAP_TYPE;

public final class LdapUserSearchSessionFactorySettings {
    public static final Setting.AffixSetting<String> SEARCH_ATTRIBUTE = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(LDAP_TYPE), "user_search.attribute",
            key -> new Setting<>(key, LdapUserSearchSessionFactorySettings.DEFAULT_USERNAME_ATTRIBUTE, Function.identity(),
                    Setting.Property.NodeScope, Setting.Property.Deprecated));

    public static final Setting.AffixSetting<String> SEARCH_BASE_DN
            = RealmSettings.simpleString(LDAP_TYPE, "user_search.base_dn", Setting.Property.NodeScope);

    public static final Setting.AffixSetting<String> SEARCH_FILTER
            = RealmSettings.simpleString(LDAP_TYPE, "user_search.filter", Setting.Property.NodeScope);

    public static final Setting.AffixSetting<LdapSearchScope> SEARCH_SCOPE = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(LDAP_TYPE), "user_search.scope",
            key -> new Setting<>(key, (String) null, (String s) -> LdapSearchScope.resolve(s, LdapSearchScope.SUB_TREE),
                    Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Boolean> POOL_ENABLED = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(LDAP_TYPE), "user_search.pool.enabled",
            key -> Setting.boolSetting(key, true, Setting.Property.NodeScope));
    private static final String DEFAULT_USERNAME_ATTRIBUTE = "uid";

    private LdapUserSearchSessionFactorySettings() {
    }

    public static Set<Setting.AffixSetting<?>> getSettings() {
        Set<Setting.AffixSetting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactorySettings.getSettings(LDAP_TYPE));
        settings.addAll(PoolingSessionFactorySettings.getSettings(LDAP_TYPE));
        settings.add(SEARCH_BASE_DN);
        settings.add(SEARCH_SCOPE);
        settings.add(SEARCH_ATTRIBUTE);
        settings.add(POOL_ENABLED);
        settings.add(SEARCH_FILTER);

        settings.addAll(SearchGroupsResolverSettings.getSettings(LDAP_TYPE));
        settings.addAll(UserAttributeGroupsResolverSettings.getSettings());

        return settings;
    }
}
