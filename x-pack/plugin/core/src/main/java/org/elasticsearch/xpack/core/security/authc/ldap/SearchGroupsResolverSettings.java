/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings.LDAP_TYPE;

public final class SearchGroupsResolverSettings {

    public static final Function<String, Setting.AffixSetting<String>> BASE_DN = RealmSettings.affixSetting(
        "group_search.base_dn",
        key -> Setting.simpleString(key, new Setting.Property[] { Setting.Property.NodeScope })
    );

    public static final Function<String, Setting.AffixSetting<LdapSearchScope>> SCOPE = RealmSettings.affixSetting(
        "group_search.scope",
        key -> new Setting<>(key, (String) null, s -> LdapSearchScope.resolve(s, LdapSearchScope.SUB_TREE), Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<String> USER_ATTRIBUTE = RealmSettings.simpleString(
        LDAP_TYPE,
        "group_search.user_attribute",
        Setting.Property.NodeScope
    );

    private static final String GROUP_SEARCH_DEFAULT_FILTER = "(&"
        + "(|(objectclass=groupOfNames)(objectclass=groupOfUniqueNames)"
        + "(objectclass=group)(objectclass=posixGroup))"
        + "(|(uniqueMember={0})(member={0})(memberUid={0})))";
    public static final Setting.AffixSetting<String> FILTER = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(LDAP_TYPE),
        "group_search.filter",
        key -> new Setting<>(key, GROUP_SEARCH_DEFAULT_FILTER, Function.identity(), Setting.Property.NodeScope)
    );

    private SearchGroupsResolverSettings() {}

    public static Set<Setting.AffixSetting<?>> getSettings(String realmType) {
        Set<Setting.AffixSetting<?>> settings = new HashSet<>();
        settings.add(BASE_DN.apply(realmType));
        settings.add(SCOPE.apply(realmType));
        if (realmType.equals(LDAP_TYPE)) {
            settings.add(FILTER);
            settings.add(USER_ATTRIBUTE);
        }
        return settings;
    }
}
