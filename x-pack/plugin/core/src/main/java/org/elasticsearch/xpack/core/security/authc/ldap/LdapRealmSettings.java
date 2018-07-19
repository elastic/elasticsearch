/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapMetaDataResolverSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.mapper.CompositeRoleMapperSettings;

import java.util.HashSet;
import java.util.Set;

public final class LdapRealmSettings {
    public static final String LDAP_TYPE = "ldap";
    public static final String AD_TYPE = "active_directory";
    public static final Setting<TimeValue> EXECUTION_TIMEOUT =
            Setting.timeSetting("timeout.execution", TimeValue.timeValueSeconds(30L), Setting.Property.NodeScope);

    private LdapRealmSettings() {}

    /**
     * @param type Either {@link #AD_TYPE} or {@link #LDAP_TYPE}
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting<?>> getSettings(String type) {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(CachingUsernamePasswordRealmSettings.getCachingSettings());
        settings.addAll(CompositeRoleMapperSettings.getSettings());
        settings.add(LdapRealmSettings.EXECUTION_TIMEOUT);
        if (AD_TYPE.equals(type)) {
            settings.addAll(ActiveDirectorySessionFactorySettings.getSettings());
        } else {
            assert LDAP_TYPE.equals(type) : "type [" + type + "] is unknown. expected one of [" + AD_TYPE + ", " + LDAP_TYPE + "]";
            settings.addAll(LdapSessionFactorySettings.getSettings());
            settings.addAll(LdapUserSearchSessionFactorySettings.getSettings());
            settings.addAll(DelegatedAuthorizationSettings.getSettings());
        }
        settings.addAll(LdapMetaDataResolverSettings.getSettings());
        return settings;
    }
}
