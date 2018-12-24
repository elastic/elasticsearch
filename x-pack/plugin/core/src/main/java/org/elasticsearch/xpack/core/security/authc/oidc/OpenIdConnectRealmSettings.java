/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.oidc;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;


public class OpenIdConnectRealmSettings {

    private OpenIdConnectRealmSettings() {
    }

    public static final String TYPE = "oidc";

    public static final Setting.AffixSetting<String> OP_NAME
        = RealmSettings.simpleString(TYPE, "op.name", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> RP_CLIENT_ID
        = RealmSettings.simpleString(TYPE, "rp.client_id", Setting.Property.NodeScope);
    public static final Setting<SecureString> RP_CLIENT_SECRET = SecureSetting.secureString("rp.client_secret", null);
    public static final Setting.AffixSetting<String> RP_REDIRECT_URI
        = RealmSettings.simpleString(TYPE, "rp.redirect_uri", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> RP_RESPONSE_TYPE
        = RealmSettings.simpleString(TYPE, "rp.response_type", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> OP_AUTHORIZATION_ENDPOINT
        = RealmSettings.simpleString(TYPE, "op.authorization_endpoint", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> OP_TOKEN_ENDPOINT
        = RealmSettings.simpleString(TYPE, "op.token_endpoint", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> OP_USERINFO_ENDPOINT
        = RealmSettings.simpleString(TYPE, "op.userinfo_endpoint", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> OP_ISSUER
        = RealmSettings.simpleString(TYPE, "op.issuer", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<List<String>> RP_REQUESTED_SCOPES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "rp.requested_scopes",
        key -> Setting.listSetting(key, Collections.singletonList("openid"), Function.identity(), Setting.Property.NodeScope));
}
