/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap.support;

import org.elasticsearch.common.settings.Setting;

import java.util.HashSet;
import java.util.Set;

public final class LdapLoadBalancingSettings {
    public static final String LOAD_BALANCE_SETTINGS = "load_balance";
    public static final String LOAD_BALANCE_TYPE_SETTING = "type";
    public static final String CACHE_TTL_SETTING = "cache_ttl";

    private LdapLoadBalancingSettings() {}

    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.add(Setting.simpleString(LOAD_BALANCE_SETTINGS + "." + LOAD_BALANCE_TYPE_SETTING, Setting.Property.NodeScope));
        settings.add(Setting.simpleString(LOAD_BALANCE_SETTINGS + "." + CACHE_TTL_SETTING, Setting.Property.NodeScope));
        return settings;
    }
}
