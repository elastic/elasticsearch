/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public final class SessionFactorySettings {

    public static final Function<String, Setting.AffixSetting<List<String>>> URLS_SETTING = RealmSettings.affixSetting(
            "url", key -> Setting.listSetting(key, Collections.emptyList(), Function.identity(), Setting.Property.NodeScope));

    public static final TimeValue TIMEOUT_DEFAULT = TimeValue.timeValueSeconds(5);
    public static final Function<String, Setting.AffixSetting<TimeValue>> TIMEOUT_TCP_CONNECTION_SETTING = RealmSettings.affixSetting(
            "timeout.tcp_connect", key -> Setting.timeSetting(key, TIMEOUT_DEFAULT, Setting.Property.NodeScope));

    public static final Function<String, Setting.AffixSetting<TimeValue>> TIMEOUT_TCP_READ_SETTING = RealmSettings.affixSetting(
            "timeout.tcp_read", key -> Setting.timeSetting(key, TIMEOUT_DEFAULT, Setting.Property.NodeScope));

    public static final Function<String, Setting.AffixSetting<TimeValue>> TIMEOUT_LDAP_SETTING = RealmSettings.affixSetting(
            "timeout.ldap_search", key -> Setting.timeSetting(key, TIMEOUT_DEFAULT, Setting.Property.NodeScope));

    public static final Function<String, Setting.AffixSetting<Boolean>> HOSTNAME_VERIFICATION_SETTING = RealmSettings.affixSetting(
            "hostname_verification", key -> Setting.boolSetting(key, true, Setting.Property.NodeScope, Setting.Property.Filtered));

    public static final Function<String, Setting.AffixSetting<Boolean>> FOLLOW_REFERRALS_SETTING = RealmSettings.affixSetting(
            "follow_referrals", key -> Setting.boolSetting(key, true, Setting.Property.NodeScope));

    public static final Function<String, Setting.AffixSetting<Boolean>> IGNORE_REFERRAL_ERRORS_SETTING = RealmSettings.affixSetting(
            "ignore_referral_errors", key -> Setting.boolSetting(key, true, Setting.Property.NodeScope));

    private SessionFactorySettings() {
    }

    public static Set<Setting.AffixSetting<?>> getSettings(String realmType) {
        Set<Setting.AffixSetting<?>> settings = new HashSet<>();
        settings.addAll(LdapLoadBalancingSettings.getSettings(realmType));
        settings.add(URLS_SETTING.apply(realmType));
        settings.add(TIMEOUT_TCP_CONNECTION_SETTING.apply(realmType));
        settings.add(TIMEOUT_TCP_READ_SETTING.apply(realmType));
        settings.add(TIMEOUT_LDAP_SETTING.apply(realmType));
        settings.add(HOSTNAME_VERIFICATION_SETTING.apply(realmType));
        settings.add(FOLLOW_REFERRALS_SETTING.apply(realmType));
        settings.add(IGNORE_REFERRAL_ERRORS_SETTING.apply(realmType));
        settings.addAll(SSLConfigurationSettings.getRealmSettings(realmType));
        return settings;
    }
}
