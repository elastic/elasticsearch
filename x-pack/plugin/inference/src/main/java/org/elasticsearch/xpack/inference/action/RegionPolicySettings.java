/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

public class RegionPolicySettings {
    /**
     * This setting is for testing only. It controls whether authorization refresh is performed.
     */
    public static final Setting<Boolean> SKIP_AUTHORIZATION_REFRESH = Setting.boolSetting(
        "xpack.inference.region_policy.authorization.skip_refresh",
        false,
        Setting.Property.NodeScope
    );

    private final boolean skipAuthorizationRefresh;

    public RegionPolicySettings(Settings settings) {
        this.skipAuthorizationRefresh = SKIP_AUTHORIZATION_REFRESH.get(settings);
    }

    public boolean skipAuthorizationRefresh() {
        return skipAuthorizationRefresh;
    }

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(SKIP_AUTHORIZATION_REFRESH);
    }
}
