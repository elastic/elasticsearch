/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.xpack.core.XPackField;

import java.util.Optional;

public final class SecurityField {

    public static final String NAME4 = XPackField.SECURITY + "4";
    public static final String NIO = XPackField.SECURITY + "-nio";
    public static final Setting<Optional<String>> USER_SETTING = new Setting<>(
        setting("user"),
        (String) null,
        Optional::ofNullable,
        Setting.Property.NodeScope
    );

    // Document and Field Level Security are Platinum+
    private static final String DLS_FLS_FEATURE_FAMILY = "security-dls-fls";
    public static final LicensedFeature.Momentary DOCUMENT_LEVEL_SECURITY_FEATURE = LicensedFeature.momentary(
        DLS_FLS_FEATURE_FAMILY,
        "dls",
        License.OperationMode.PLATINUM
    );
    public static final LicensedFeature.Momentary FIELD_LEVEL_SECURITY_FEATURE = LicensedFeature.momentary(
        DLS_FLS_FEATURE_FAMILY,
        "fls",
        License.OperationMode.PLATINUM
    );

    private SecurityField() {
        // Assert that DLS and FLS have identical license requirement so that license check can be simplified to just
        // use DLS. But they are handled separately when it comes to usage tracking.
        assert DOCUMENT_LEVEL_SECURITY_FEATURE.getMinimumOperationMode() == FIELD_LEVEL_SECURITY_FEATURE.getMinimumOperationMode()
            : "dls and fls must have the same license requirement";
        assert DOCUMENT_LEVEL_SECURITY_FEATURE.isNeedsActive() == FIELD_LEVEL_SECURITY_FEATURE.isNeedsActive()
            : "dls and fls must have the same license requirement";
    }

    public static String setting(String setting) {
        assert setting != null && setting.startsWith(".") == false;
        return settingPrefix() + setting;
    }

    public static String settingPrefix() {
        return XPackField.featureSettingPrefix(XPackField.SECURITY) + ".";
    }
}
