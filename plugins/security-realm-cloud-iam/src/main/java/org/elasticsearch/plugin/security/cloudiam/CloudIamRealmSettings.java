/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License v 1".
 */
package org.elasticsearch.plugin.security.cloudiam;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.ArrayList;
import java.util.List;

public final class CloudIamRealmSettings {
    public static final String TYPE = "cloud_iam";

    public static final Setting.AffixSetting<String> AUTH_HEADER = RealmSettings.simpleString(
        TYPE,
        "auth.header",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<String> SIGNED_HEADER = RealmSettings.simpleString(
        TYPE,
        "auth.signed_header",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<String> MOCK_SIGNATURE = RealmSettings.simpleString(
        TYPE,
        "mock.signature",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<String> MOCK_ARN_TEMPLATE = RealmSettings.simpleString(
        TYPE,
        "mock.arn_template",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<String> MOCK_ACCOUNT_ID = RealmSettings.simpleString(
        TYPE,
        "mock.account_id",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<List<String>> MOCK_ROLES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "mock.roles",
        key -> Setting.stringListSetting(key, List.of("read_only"), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<String> AUTH_MODE = RealmSettings.simpleString(
        TYPE,
        "auth.mode",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<Boolean> ROLE_MAPPING_ENABLED = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "role_mapping.enabled",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Boolean> ALLOW_ASSUMED_ROLE = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "auth.allow_assumed_role",
        key -> Setting.boolSetting(key, false, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> ALLOWED_SKEW = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "auth.allowed_time_skew",
        key -> Setting.timeSetting(key, TimeValue.timeValueMinutes(5), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<ByteSizeValue> SIGNED_HEADER_MAX_BYTES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "auth.signed_header_max_bytes",
        key -> Setting.byteSizeSetting(key, ByteSizeValue.ofKb(8), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<String> IAM_ENDPOINT = RealmSettings.simpleString(
        TYPE,
        "iam.endpoint",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<String> IAM_REGION = RealmSettings.simpleString(
        TYPE,
        "iam.region",
        Setting.Property.NodeScope
    );
    public static final Setting.AffixSetting<TimeValue> IAM_CONNECT_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "iam.timeout.connect",
        key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(1), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> IAM_READ_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "iam.timeout.read",
        key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(2), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> CACHE_TTL = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "cache.ttl",
        key -> Setting.timeSetting(key, TimeValue.timeValueMinutes(5), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> CACHE_MAX_ENTRIES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "cache.max_entries",
        key -> Setting.intSetting(key, 10000, 0, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> NEGATIVE_CACHE_TTL = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "cache.negative_ttl",
        key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(20), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<TimeValue> NONCE_TTL = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "replay.nonce_ttl",
        key -> Setting.timeSetting(key, TimeValue.timeValueMinutes(5), Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<Integer> NONCE_MAX_ENTRIES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "replay.nonce_max_entries",
        key -> Setting.intSetting(key, 50000, 0, Setting.Property.NodeScope)
    );

    private CloudIamRealmSettings() {}

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(RealmSettings.getStandardSettings(TYPE));
        settings.add(AUTH_HEADER);
        settings.add(SIGNED_HEADER);
        settings.add(AUTH_MODE);
        settings.add(ROLE_MAPPING_ENABLED);
        settings.add(ALLOW_ASSUMED_ROLE);
        settings.add(ALLOWED_SKEW);
        settings.add(SIGNED_HEADER_MAX_BYTES);
        settings.add(IAM_ENDPOINT);
        settings.add(IAM_REGION);
        settings.add(IAM_CONNECT_TIMEOUT);
        settings.add(IAM_READ_TIMEOUT);
        settings.add(CACHE_TTL);
        settings.add(CACHE_MAX_ENTRIES);
        settings.add(NEGATIVE_CACHE_TTL);
        settings.add(NONCE_TTL);
        settings.add(NONCE_MAX_ENTRIES);
        settings.add(MOCK_SIGNATURE);
        settings.add(MOCK_ARN_TEMPLATE);
        settings.add(MOCK_ACCOUNT_ID);
        settings.add(MOCK_ROLES);
        return List.copyOf(settings);
    }
}
