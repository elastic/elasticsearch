/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class SearchGroupsResolverSettings {
    public static final Setting<String> BASE_DN = Setting.simpleString("group_search.base_dn",
            Setting.Property.NodeScope);
    public static final Setting<LdapSearchScope> SCOPE = new Setting<>("group_search.scope", (String) null,
            s -> LdapSearchScope.resolve(s, LdapSearchScope.SUB_TREE), Setting.Property.NodeScope);
    public static final Setting<String> USER_ATTRIBUTE = Setting.simpleString(
            "group_search.user_attribute", Setting.Property.NodeScope);
    private static final String GROUP_SEARCH_DEFAULT_FILTER = "(&" +
            "(|(objectclass=groupOfNames)(objectclass=groupOfUniqueNames)" +
            "(objectclass=group)(objectclass=posixGroup))" +
            "(|(uniqueMember={0})(member={0})(memberUid={0})))";
    public static final Setting<String> FILTER = new Setting<>("group_search.filter",
            GROUP_SEARCH_DEFAULT_FILTER, Function.identity(), Setting.Property.NodeScope);

    private SearchGroupsResolverSettings() {}

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.add(BASE_DN);
        settings.add(FILTER);
        settings.add(USER_ATTRIBUTE);
        settings.add(SCOPE);
        return settings;
    }
}
