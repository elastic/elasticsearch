/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.esnative;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;

import java.util.HashSet;
import java.util.Set;

public final class NativeRealmSettings {
    public static final String TYPE = "native";
    public static final String DEFAULT_NAME = "default_native";

    /**
     * This setting is never registered by the security plugin - in order to disable the native user APIs
     * another plugin must register it as a boolean setting and cause it to be set to `false`.
     *
     * If this setting is set to <code>false</code> then
     * <ul>
     *     <li>the Rest APIs for native user management are disabled.</li>
     *     <li>the default native realm will <em>not</em> be automatically configured.</li>
     *     <li>it is not possible to configure a native realm.</li>
     * </ul>
     */
    public static final String NATIVE_USERS_ENABLED = "xpack.security.authc.native_users.enabled";

    private NativeRealmSettings() {}

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = new HashSet<>(CachingUsernamePasswordRealmSettings.getSettings(TYPE));
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        return set;
    }
}
