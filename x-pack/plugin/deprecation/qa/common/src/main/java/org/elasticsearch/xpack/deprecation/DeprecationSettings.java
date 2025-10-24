/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.common.settings.Setting;

public class DeprecationSettings {
    public static final Setting<Boolean> TEST_DEPRECATED_SETTING_TRUE1 = Setting.boolSetting(
        "test.setting.deprecated.true1",
        true,
        Setting.Property.NodeScope,
        Setting.Property.DeprecatedWarning,
        Setting.Property.Dynamic
    );
    public static final Setting<Boolean> TEST_DEPRECATED_SETTING_TRUE2 = Setting.boolSetting(
        "test.setting.deprecated.true2",
        true,
        Setting.Property.NodeScope,
        Setting.Property.DeprecatedWarning,
        Setting.Property.Dynamic
    );
    public static final Setting<Boolean> TEST_DEPRECATED_SETTING_TRUE3 = Setting.boolSetting(
        "test.setting.deprecated.true3",
        true,
        Setting.Property.NodeScope,
        Setting.Property.Deprecated,
        Setting.Property.Dynamic
    );
    public static final Setting<Boolean> TEST_NOT_DEPRECATED_SETTING = Setting.boolSetting(
        "test.setting.not_deprecated",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final String DEPRECATED_ENDPOINT = "[/_test_cluster/deprecated_settings] exists for deprecated tests";
    public static final String DEPRECATED_USAGE = "[deprecated_settings] usage is deprecated. use [settings] instead";
    public static final String DEPRECATED_WARN_USAGE =
        "[deprecated_warn_settings] usage is deprecated but won't be breaking in next version";
    public static final String COMPATIBLE_API_USAGE = "You are using a compatible API for this request";
}
