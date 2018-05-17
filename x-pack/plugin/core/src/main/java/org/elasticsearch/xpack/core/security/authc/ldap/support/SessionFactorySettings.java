/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public final class SessionFactorySettings {
    public static final String URLS_SETTING = "url";
    public static final String TIMEOUT_TCP_CONNECTION_SETTING = "timeout.tcp_connect";
    public static final String TIMEOUT_TCP_READ_SETTING = "timeout.tcp_read";
    public static final String TIMEOUT_LDAP_SETTING = "timeout.ldap_search";
    public static final String HOSTNAME_VERIFICATION_SETTING = "hostname_verification";
    public static final String FOLLOW_REFERRALS_SETTING = "follow_referrals";
    public static final Setting<Boolean> IGNORE_REFERRAL_ERRORS_SETTING = Setting.boolSetting(
            "ignore_referral_errors", true, Setting.Property.NodeScope);
    public static final TimeValue TIMEOUT_DEFAULT = TimeValue.timeValueSeconds(5);

    private SessionFactorySettings() {}

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.addAll(LdapLoadBalancingSettings.getSettings());
        settings.add(Setting.listSetting(URLS_SETTING, Collections.emptyList(), Function.identity(),
                Setting.Property.NodeScope));
        settings.add(Setting.timeSetting(TIMEOUT_TCP_CONNECTION_SETTING, TIMEOUT_DEFAULT, Setting.Property.NodeScope));
        settings.add(Setting.timeSetting(TIMEOUT_TCP_READ_SETTING, TIMEOUT_DEFAULT, Setting.Property.NodeScope));
        settings.add(Setting.timeSetting(TIMEOUT_LDAP_SETTING, TIMEOUT_DEFAULT, Setting.Property.NodeScope));
        settings.add(Setting.boolSetting(HOSTNAME_VERIFICATION_SETTING, true, Setting.Property.NodeScope, Setting.Property.Filtered));
        settings.add(Setting.boolSetting(FOLLOW_REFERRALS_SETTING, true, Setting.Property.NodeScope));
        settings.add(IGNORE_REFERRAL_ERRORS_SETTING);
        settings.addAll(SSLConfigurationSettings.withPrefix("ssl.").getAllSettings());
        return settings;
    }
}
