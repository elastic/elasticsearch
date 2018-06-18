/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.ldap;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;

import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.common.settings.SecureSetting.secureString;

public final class PoolingSessionFactorySettings {
    public static final TimeValue DEFAULT_HEALTH_CHECK_INTERVAL = TimeValue.timeValueSeconds(60L);
    public static final Setting<String> BIND_DN = Setting.simpleString("bind_dn", Setting.Property.NodeScope, Setting.Property.Filtered);
    public static final Setting<SecureString> LEGACY_BIND_PASSWORD = new Setting<>("bind_password", "", SecureString::new,
            Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Deprecated);
    public static final Setting<SecureString> SECURE_BIND_PASSWORD = secureString("secure_bind_password", LEGACY_BIND_PASSWORD);

    public static final int DEFAULT_CONNECTION_POOL_INITIAL_SIZE = 0;
    public static final Setting<Integer> POOL_INITIAL_SIZE = Setting.intSetting("user_search.pool.initial_size",
            DEFAULT_CONNECTION_POOL_INITIAL_SIZE, 0, Setting.Property.NodeScope);
    public static final int DEFAULT_CONNECTION_POOL_SIZE = 20;
    public static final Setting<Integer> POOL_SIZE = Setting.intSetting("user_search.pool.size",
            DEFAULT_CONNECTION_POOL_SIZE, 1, Setting.Property.NodeScope);
    public static final Setting<TimeValue> HEALTH_CHECK_INTERVAL = Setting.timeSetting("user_search.pool.health_check.interval",
            DEFAULT_HEALTH_CHECK_INTERVAL, Setting.Property.NodeScope);
    public static final Setting<Boolean> HEALTH_CHECK_ENABLED = Setting.boolSetting("user_search.pool.health_check.enabled",
            true, Setting.Property.NodeScope);
    public static final Setting<Optional<String>> HEALTH_CHECK_DN = new Setting<>("user_search.pool.health_check.dn", (String) null,
            Optional::ofNullable, Setting.Property.NodeScope);

    private PoolingSessionFactorySettings() {}

    public static Set<Setting<?>> getSettings() {
        return Sets.newHashSet(POOL_INITIAL_SIZE, POOL_SIZE, HEALTH_CHECK_ENABLED, HEALTH_CHECK_INTERVAL, HEALTH_CHECK_DN, BIND_DN,
                SECURE_BIND_PASSWORD, LEGACY_BIND_PASSWORD);
    }
}
