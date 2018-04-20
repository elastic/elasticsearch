/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class LdapUserSearchSessionFactorySettings {
    public static final Setting<String> SEARCH_ATTRIBUTE = new Setting<>("user_search.attribute",
            LdapUserSearchSessionFactorySettings.DEFAULT_USERNAME_ATTRIBUTE,
            Function.identity(), Setting.Property.NodeScope, Setting.Property.Deprecated);
    public static final Setting<String> SEARCH_BASE_DN = Setting.simpleString("user_search.base_dn", Setting.Property.NodeScope);
    public static final Setting<String> SEARCH_FILTER = Setting.simpleString("user_search.filter", Setting.Property.NodeScope);
    public static final Setting<LdapSearchScope> SEARCH_SCOPE = new Setting<>("user_search.scope", (String) null,
            s -> LdapSearchScope.resolve(s, LdapSearchScope.SUB_TREE), Setting.Property.NodeScope);
    public static final Setting<Boolean> POOL_ENABLED = Setting.boolSetting("user_search.pool.enabled", true, Setting.Property.NodeScope);
    private static final String DEFAULT_USERNAME_ATTRIBUTE = "uid";

    private LdapUserSearchSessionFactorySettings() {}

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(SessionFactorySettings.getSettings());
        settings.addAll(PoolingSessionFactorySettings.getSettings());
        settings.add(SEARCH_BASE_DN);
        settings.add(SEARCH_SCOPE);
        settings.add(SEARCH_ATTRIBUTE);
        settings.add(POOL_ENABLED);
        settings.add(SEARCH_FILTER);

        settings.addAll(SearchGroupsResolverSettings.getSettings());
        settings.addAll(UserAttributeGroupsResolverSettings.getSettings());

        return settings;
    }
}
