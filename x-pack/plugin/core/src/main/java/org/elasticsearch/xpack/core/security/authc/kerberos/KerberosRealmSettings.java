/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc.kerberos;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.TimeValue;

import java.util.HashSet;
import java.util.Set;

/**
 * Kerberos Realm settings
 */
public final class KerberosRealmSettings {
    public static final String TYPE = "kerberos";

    /**
     * Kerberos Key tab for Elasticsearch HTTP Service and Kibana HTTP Service<br>
     * Uses single key tab for multiple service accounts.
     */
    public static final Setting<String> HTTP_SERVICE_KEYTAB_PATH =
            Setting.simpleString("http.service.keytab.path", Setting.Property.NodeScope);
    public static final Setting<Boolean> SETTING_KRB_DEBUG_ENABLE =
            Setting.boolSetting("krb.debug", Boolean.FALSE, Setting.Property.Dynamic, Property.NodeScope);
    // Cache
    private static final TimeValue DEFAULT_TTL = TimeValue.timeValueMinutes(20);
    public static final Setting<TimeValue> CACHE_TTL_SETTING = Setting.timeSetting("cache.ttl", DEFAULT_TTL, Setting.Property.NodeScope);
    private static final int DEFAULT_MAX_USERS = 100_000; // 100k users
    public static final Setting<Integer> CACHE_MAX_USERS_SETTING =
            Setting.intSetting("cache.max_users", DEFAULT_MAX_USERS, Setting.Property.NodeScope);

    private KerberosRealmSettings() {
        /* Empty private constructor */
    }

    /**
     * @return Set of {@link Setting}s for {@value #TYPE}
     */
    public static Set<Setting<?>> getSettings() {
        Set<Setting<?>> settings = new HashSet<>();
        settings.add(HTTP_SERVICE_KEYTAB_PATH);
        settings.add(CACHE_TTL_SETTING);
        settings.add(CACHE_MAX_USERS_SETTING);
        settings.add(SETTING_KRB_DEBUG_ENABLE);
        return settings;
    }
}
