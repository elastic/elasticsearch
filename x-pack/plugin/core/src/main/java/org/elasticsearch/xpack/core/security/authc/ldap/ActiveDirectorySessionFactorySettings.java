/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings.AD_TYPE;

public final class ActiveDirectorySessionFactorySettings {
    private static final String AD_DOMAIN_NAME_SETTING_KEY = "domain_name";
    public static final Setting.AffixSetting<String> AD_DOMAIN_NAME_SETTING
            = RealmSettings.simpleString(AD_TYPE, AD_DOMAIN_NAME_SETTING_KEY, Setting.Property.NodeScope);

    public static final String AD_GROUP_SEARCH_BASEDN_SETTING = "group_search.base_dn";
    public static final String AD_GROUP_SEARCH_SCOPE_SETTING = "group_search.scope";

    private static final String AD_USER_SEARCH_BASEDN_SETTING_KEY = "user_search.base_dn";
    public static final Setting.AffixSetting<String> AD_USER_SEARCH_BASEDN_SETTING
            = RealmSettings.simpleString(AD_TYPE, AD_USER_SEARCH_BASEDN_SETTING_KEY, Setting.Property.NodeScope);

    private static final String AD_USER_SEARCH_FILTER_SETTING_KEY = "user_search.filter";
    public static final Setting.AffixSetting<String> AD_USER_SEARCH_FILTER_SETTING
            = RealmSettings.simpleString(AD_TYPE, AD_USER_SEARCH_FILTER_SETTING_KEY, Setting.Property.NodeScope);

    private static final String AD_UPN_USER_SEARCH_FILTER_SETTING_KEY = "user_search.upn_filter";
    public static final Setting.AffixSetting<String> AD_UPN_USER_SEARCH_FILTER_SETTING
            = RealmSettings.simpleString(AD_TYPE, AD_UPN_USER_SEARCH_FILTER_SETTING_KEY, Setting.Property.NodeScope);

    private static final String AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING_KEY = "user_search.down_level_filter";
    public static final Setting.AffixSetting<String> AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING
            = RealmSettings.simpleString(AD_TYPE, AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING_KEY, Setting.Property.NodeScope);

    private static final String AD_USER_SEARCH_SCOPE_SETTING_KEY = "user_search.scope";
    public static final Setting.AffixSetting<String> AD_USER_SEARCH_SCOPE_SETTING
            = RealmSettings.simpleString(AD_TYPE, AD_USER_SEARCH_SCOPE_SETTING_KEY, Setting.Property.NodeScope);

    public static final Setting.AffixSetting<Integer> AD_LDAP_PORT_SETTING = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(AD_TYPE), "port.ldap", key -> Setting.intSetting(key, 389, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Integer> AD_LDAPS_PORT_SETTING = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(AD_TYPE), "port.ldaps", key -> Setting.intSetting(key, 636, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Integer> AD_GC_LDAP_PORT_SETTING = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(AD_TYPE), "port.gc_ldap", key -> Setting.intSetting(key, 3268, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<Integer> AD_GC_LDAPS_PORT_SETTING = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(AD_TYPE), "port.gc_ldaps", key -> Setting.intSetting(key, 3269, Setting.Property.NodeScope));

    public static final String POOL_ENABLED_SUFFIX = "user_search.pool.enabled";
    public static final Setting.AffixSetting<Boolean> POOL_ENABLED = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(AD_TYPE), POOL_ENABLED_SUFFIX,
            key -> {
                if (key.endsWith(POOL_ENABLED_SUFFIX)) {
                    final String bindDnKey = key.substring(0, key.length() - POOL_ENABLED_SUFFIX.length())
                            + PoolingSessionFactorySettings.BIND_DN_SUFFIX;
                    return Setting.boolSetting(key, settings -> Boolean.toString(settings.keySet().contains(bindDnKey)),
                            Setting.Property.NodeScope);
                } else {
                    return Setting.boolSetting(key, false, Setting.Property.NodeScope);
                }
            });

    private ActiveDirectorySessionFactorySettings() {
    }

    public static Set<Setting.AffixSetting<?>> getSettings() {
        Set<Setting.AffixSetting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactorySettings.getSettings(AD_TYPE));
        settings.add(AD_DOMAIN_NAME_SETTING);
        settings.add(RealmSettings.simpleString(AD_TYPE, AD_GROUP_SEARCH_BASEDN_SETTING, Setting.Property.NodeScope));
        settings.add(RealmSettings.simpleString(AD_TYPE, AD_GROUP_SEARCH_SCOPE_SETTING, Setting.Property.NodeScope));
        settings.add(AD_USER_SEARCH_BASEDN_SETTING);
        settings.add(AD_USER_SEARCH_FILTER_SETTING);
        settings.add(AD_UPN_USER_SEARCH_FILTER_SETTING);
        settings.add(AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING);
        settings.add(AD_USER_SEARCH_SCOPE_SETTING);
        settings.add(AD_LDAP_PORT_SETTING);
        settings.add(AD_LDAPS_PORT_SETTING);
        settings.add(AD_GC_LDAP_PORT_SETTING);
        settings.add(AD_GC_LDAPS_PORT_SETTING);
        settings.add(POOL_ENABLED);
        settings.addAll(PoolingSessionFactorySettings.getSettings(AD_TYPE));
        return settings;
    }
}
