/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Optional;

public final class SecurityField {

    public static final String NAME4 = XPackField.SECURITY + "4";
    public static final String NIO = XPackField.SECURITY + "-nio";
    public static final Setting<Optional<String>> USER_SETTING =
            new Setting<>(setting("user"), (String) null, Optional::ofNullable, Setting.Property.NodeScope);

    private SecurityField() {}

    public static String setting(String setting) {
        assert setting != null && setting.startsWith(".") == false;
        return settingPrefix() + setting;
    }

    public static String settingPrefix() {
        return XPackField.featureSettingPrefix(XPackField.SECURITY) + ".";
    }
}
