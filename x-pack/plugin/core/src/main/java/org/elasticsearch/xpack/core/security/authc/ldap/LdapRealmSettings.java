/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapMetadataResolverSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.mapper.CompositeRoleMapperSettings;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class LdapRealmSettings {
    public static final String LDAP_TYPE = "ldap";
    public static final String AD_TYPE = "active_directory";

    public static final String TIMEOUT_EXECUTION_SUFFIX = "timeout.execution";
    public static final Function<String, Setting.AffixSetting<TimeValue>> EXECUTION_TIMEOUT = type ->
            Setting.affixKeySetting(RealmSettings.realmSettingPrefix(type), TIMEOUT_EXECUTION_SUFFIX,
                    key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(30L), Setting.Property.NodeScope));

    private LdapRealmSettings() {
    }

    /**
     * @param type Either {@link #AD_TYPE} or {@link #LDAP_TYPE}
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting.AffixSetting<?>> getSettings(String type) {
        Set<Setting.AffixSetting<?>> settings = new HashSet<>();
        settings.addAll(CachingUsernamePasswordRealmSettings.getSettings(type));
        settings.addAll(CompositeRoleMapperSettings.getSettings(type));
        settings.add(LdapRealmSettings.EXECUTION_TIMEOUT.apply(type));
        if (AD_TYPE.equals(type)) {
            settings.addAll(ActiveDirectorySessionFactorySettings.getSettings());
        } else {
            assert LDAP_TYPE.equals(type) : "type [" + type + "] is unknown. expected one of [" + AD_TYPE + ", " + LDAP_TYPE + "]";
            settings.addAll(LdapSessionFactorySettings.getSettings());
            settings.addAll(LdapUserSearchSessionFactorySettings.getSettings());
            settings.addAll(DelegatedAuthorizationSettings.getSettings(type));
        }
        settings.addAll(LdapMetadataResolverSettings.getSettings(type));
        settings.addAll(RealmSettings.getStandardSettings(type));
        return settings;
    }
}
