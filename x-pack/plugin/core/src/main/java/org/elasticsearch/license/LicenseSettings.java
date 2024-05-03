/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Settings related to the license.
 */
public abstract class LicenseSettings {
    public static final Setting<License.LicenseType> SELF_GENERATED_LICENSE_TYPE = new Setting<>(
        "xpack.license.self_generated.type",
        License.LicenseType.BASIC.getTypeName(),
        (s) -> {
            final License.LicenseType type = License.LicenseType.parse(s);
            return SelfGeneratedLicense.validateSelfGeneratedType(type);
        },
        Setting.Property.NodeScope
    );
    public static final List<License.LicenseType> ALLOWABLE_UPLOAD_TYPES = getAllowableUploadTypes();
    public static final Setting<List<License.LicenseType>> ALLOWED_LICENSE_TYPES_SETTING = Setting.listSetting(
        "xpack.license.upload.types",
        ALLOWABLE_UPLOAD_TYPES.stream().map(License.LicenseType::getTypeName).toList(),
        License.LicenseType::parse,
        LicenseSettings::validateUploadTypesSetting,
        Setting.Property.NodeScope
    );
    // pkg private for tests
    static final TimeValue NON_BASIC_SELF_GENERATED_LICENSE_DURATION = TimeValue.timeValueHours(30 * 24);
    static final Set<License.LicenseType> VALID_TRIAL_TYPES = Set.of(
        License.LicenseType.GOLD,
        License.LicenseType.PLATINUM,
        License.LicenseType.ENTERPRISE,
        License.LicenseType.TRIAL
    );
    /**
     * Period before the license expires when warning starts being added to the response header
     */
    static final TimeValue LICENSE_EXPIRATION_WARNING_PERIOD = TimeValue.timeValueDays(7);
    static final long BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS = XPackInfoResponse.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
    /**
     * Max number of nodes licensed by generated trial license
     */
    static final int SELF_GENERATED_LICENSE_MAX_NODES = 1000;
    static final int SELF_GENERATED_LICENSE_MAX_RESOURCE_UNITS = SELF_GENERATED_LICENSE_MAX_NODES;

    private static List<License.LicenseType> getAllowableUploadTypes() {
        return Stream.of(License.LicenseType.values()).filter(t -> t != License.LicenseType.BASIC).toList();
    }

    private static void validateUploadTypesSetting(List<License.LicenseType> value) {
        if (ALLOWABLE_UPLOAD_TYPES.containsAll(value) == false) {
            throw new IllegalArgumentException(
                "Invalid value ["
                    + value.stream().map(License.LicenseType::getTypeName).collect(Collectors.joining(","))
                    + "] for "
                    + ALLOWED_LICENSE_TYPES_SETTING.getKey()
                    + ", allowed values are ["
                    + ALLOWABLE_UPLOAD_TYPES.stream().map(License.LicenseType::getTypeName).collect(Collectors.joining(","))
                    + "]"
            );
        }
    }
}
