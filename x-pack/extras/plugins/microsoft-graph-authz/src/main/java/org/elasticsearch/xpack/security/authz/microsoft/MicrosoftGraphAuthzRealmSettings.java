/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.ArrayList;
import java.util.List;

public class MicrosoftGraphAuthzRealmSettings {
    public static final String REALM_TYPE = "microsoft_graph";

    public static final Setting.AffixSetting<String> CLIENT_ID = RealmSettings.simpleString(
        REALM_TYPE,
        "client_id",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<SecureString> CLIENT_SECRET = RealmSettings.secureString(REALM_TYPE, "client_secret");

    public static final Setting.AffixSetting<String> TENANT_ID = RealmSettings.simpleString(
        REALM_TYPE,
        "tenant_id",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<String> ACCESS_TOKEN_HOST = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(REALM_TYPE),
        "access_token_host",
        key -> Setting.simpleString(key, "https://login.microsoftonline.com", Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<String> API_HOST = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(REALM_TYPE),
        "graph_host",
        key -> Setting.simpleString(key, "https://graph.microsoft.com/v1.0", Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> HTTP_REQUEST_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(REALM_TYPE),
        "http_request_timeout",
        key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(10), Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> EXECUTION_TIMEOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(REALM_TYPE),
        "execution_timeout",
        key -> Setting.timeSetting(key, TimeValue.timeValueSeconds(30), Setting.Property.NodeScope)
    );

    public static List<Setting<?>> getSettings() {
        var settings = new ArrayList<Setting<?>>(RealmSettings.getStandardSettings(REALM_TYPE));
        settings.add(CLIENT_ID);
        settings.add(CLIENT_SECRET);
        settings.add(TENANT_ID);
        settings.add(ACCESS_TOKEN_HOST);
        settings.add(API_HOST);
        settings.add(HTTP_REQUEST_TIMEOUT);
        settings.add(EXECUTION_TIMEOUT);

        return settings;
    }
}
