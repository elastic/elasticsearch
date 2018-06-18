/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;

import java.util.HashSet;
import java.util.Set;

public final class ActiveDirectorySessionFactorySettings {
    public static final String AD_DOMAIN_NAME_SETTING = "domain_name";
    public static final String AD_GROUP_SEARCH_BASEDN_SETTING = "group_search.base_dn";
    public static final String AD_GROUP_SEARCH_SCOPE_SETTING = "group_search.scope";
    public static final String AD_USER_SEARCH_BASEDN_SETTING = "user_search.base_dn";
    public static final String AD_USER_SEARCH_FILTER_SETTING = "user_search.filter";
    public static final String AD_UPN_USER_SEARCH_FILTER_SETTING = "user_search.upn_filter";
    public static final String AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING = "user_search.down_level_filter";
    public static final String AD_USER_SEARCH_SCOPE_SETTING = "user_search.scope";
    public static final Setting<Integer> AD_LDAP_PORT_SETTING = Setting.intSetting("port.ldap", 389, Setting.Property.NodeScope);
    public static final Setting<Integer> AD_LDAPS_PORT_SETTING = Setting.intSetting("port.ldaps", 636, Setting.Property.NodeScope);
    public static final Setting<Integer> AD_GC_LDAP_PORT_SETTING = Setting.intSetting("port.gc_ldap", 3268, Setting.Property.NodeScope);
    public static final Setting<Integer> AD_GC_LDAPS_PORT_SETTING = Setting.intSetting("port.gc_ldaps", 3269, Setting.Property.NodeScope);
    public static final Setting<Boolean> POOL_ENABLED = Setting.boolSetting("user_search.pool.enabled",
            settings -> Boolean.toString(PoolingSessionFactorySettings.BIND_DN.exists(settings)), Setting.Property.NodeScope);

    private ActiveDirectorySessionFactorySettings() {}

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactorySettings.getSettings());
        settings.add(Setting.simpleString(AD_DOMAIN_NAME_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_GROUP_SEARCH_BASEDN_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_GROUP_SEARCH_SCOPE_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_BASEDN_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_FILTER_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_UPN_USER_SEARCH_FILTER_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(AD_USER_SEARCH_SCOPE_SETTING, Setting.Property.NodeScope));
        settings.add(AD_LDAP_PORT_SETTING);
        settings.add(AD_LDAPS_PORT_SETTING);
        settings.add(AD_GC_LDAP_PORT_SETTING);
        settings.add(AD_GC_LDAPS_PORT_SETTING);
        settings.add(POOL_ENABLED);
        settings.addAll(PoolingSessionFactorySettings.getSettings());
        return settings;
    }
}
