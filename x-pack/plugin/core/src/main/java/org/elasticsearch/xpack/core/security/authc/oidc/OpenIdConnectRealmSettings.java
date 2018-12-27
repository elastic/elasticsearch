/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.oidc;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;


public class OpenIdConnectRealmSettings {

    private OpenIdConnectRealmSettings() {
    }

    public static final String TYPE = "oidc";

    public static final Setting.AffixSetting<String> OP_NAME
        = RealmSettings.simpleString(TYPE, "op.name", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> RP_CLIENT_ID
        = RealmSettings.simpleString(TYPE, "rp.client_id", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<SecureString> RP_CLIENT_SECRET
        = RealmSettings.secureString(TYPE, "rp.client_secret");
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

    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet(
            OP_NAME, RP_CLIENT_ID, RP_REDIRECT_URI, RP_RESPONSE_TYPE, RP_REQUESTED_SCOPES, RP_CLIENT_SECRET,
            OP_AUTHORIZATION_ENDPOINT, OP_TOKEN_ENDPOINT, OP_USERINFO_ENDPOINT, OP_ISSUER);
        set.addAll(DelegatedAuthorizationSettings.getSettings(TYPE));
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        return set;
    }
}
