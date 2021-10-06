/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.ldap.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class LdapLoadBalancingSettings {

    public static final Function<String, Setting.AffixSetting<String>> LOAD_BALANCE_TYPE_SETTING = RealmSettings.affixSetting(
            "load_balance.type", key -> Setting.simpleString(key, Setting.Property.NodeScope));

    private static final TimeValue CACHE_TTL_DEFAULT = TimeValue.timeValueHours(1L);
    public static final Function<String, Setting.AffixSetting<TimeValue>> CACHE_TTL_SETTING = RealmSettings.affixSetting(
            "load_balance.cache_ttl", key -> Setting.timeSetting(key, CACHE_TTL_DEFAULT, Setting.Property.NodeScope));

    private LdapLoadBalancingSettings() {
    }

    public static Set<Setting.AffixSetting<?>> getSettings(String realmType) {
        Set<Setting.AffixSetting<?>> settings = new HashSet<>();
        settings.add(LOAD_BALANCE_TYPE_SETTING.apply(realmType));
        settings.add(CACHE_TTL_SETTING.apply(realmType));
        return settings;
    }
}
