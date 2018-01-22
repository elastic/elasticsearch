/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactorySettings;

import java.util.HashSet;
import java.util.Set;

public final class ActiveDirectorySessionFactorySettings {
    static final String AD_DOMAIN_NAME_SETTING = "domain_name";
    static final String AD_GROUP_SEARCH_BASEDN_SETTING = "group_search.base_dn";
    static final String AD_GROUP_SEARCH_SCOPE_SETTING = "group_search.scope";
    static final String AD_USER_SEARCH_BASEDN_SETTING = "user_search.base_dn";
    static final String AD_USER_SEARCH_FILTER_SETTING = "user_search.filter";
    static final String AD_UPN_USER_SEARCH_FILTER_SETTING = "user_search.upn_filter";
    static final String AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING = "user_search.down_level_filter";
    static final String AD_USER_SEARCH_SCOPE_SETTING = "user_search.scope";
    static final Setting<Boolean> POOL_ENABLED = Setting.boolSetting("user_search.pool.enabled",
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
        settings.add(POOL_ENABLED);
        settings.addAll(PoolingSessionFactorySettings.getSettings());
        return settings;
    }
}
