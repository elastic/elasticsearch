/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.kerberos;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;

import java.util.Set;

/**
 * Kerberos Realm settings
 */
public final class KerberosRealmSettings {
    public static final String TYPE = "kerberos";

    /**
     * Kerberos key tab for Elasticsearch service<br>
     * Uses single key tab for multiple service accounts.
     */
    public static final Setting.AffixSetting<String> HTTP_SERVICE_KEYTAB_PATH = RealmSettings.simpleString(TYPE,
        "keytab.path", Property.NodeScope);

    public static final Setting.AffixSetting<Boolean> SETTING_KRB_DEBUG_ENABLE = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "krb.debug", key -> Setting.boolSetting(key, Boolean.FALSE, Property.NodeScope));

    public static final Setting.AffixSetting<Boolean> SETTING_REMOVE_REALM_NAME = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "remove_realm_name", key -> Setting.boolSetting(key, Boolean.FALSE, Property.NodeScope));

    // Cache
    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueMinutes(20);
    private static final int DEFAULT_MAX_USERS = 100_000; // 100k users
    public static final Setting.AffixSetting<TimeValue> CACHE_TTL_SETTING = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "cache.ttl", key -> Setting.timeSetting(key, DEFAULT_TTL, Setting.Property.NodeScope));

    public static final Setting.AffixSetting<Integer> CACHE_MAX_USERS_SETTING = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "cache.max_users", key -> Setting.intSetting(key, DEFAULT_MAX_USERS, Property.NodeScope));

    private KerberosRealmSettings() {
    }

    /**
     * @return the valid set of {@link Setting}s for a {@value #TYPE} realm
     */
    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> settings = Sets.newHashSet(HTTP_SERVICE_KEYTAB_PATH, CACHE_TTL_SETTING, CACHE_MAX_USERS_SETTING,
                SETTING_KRB_DEBUG_ENABLE, SETTING_REMOVE_REALM_NAME);
        settings.addAll(DelegatedAuthorizationSettings.getSettings(TYPE));
        settings.addAll(RealmSettings.getStandardSettings(TYPE));
        return settings;
    }
}
