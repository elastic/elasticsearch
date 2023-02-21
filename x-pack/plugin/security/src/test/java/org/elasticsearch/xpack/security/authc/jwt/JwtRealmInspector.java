/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.ClaimSetting;

import java.net.URI;
import java.util.List;

class JwtRealmInspector {

    private JwtRealmInspector() {}

    public static JwtRealmSettings.TokenType getTokenType(JwtRealm realm) {
        return realm.getConfig().getSetting(JwtRealmSettings.TOKEN_TYPE);
    }

    public static String getJwkSetPath(JwtRealm realm) {
        return realm.getConfig().getSetting(JwtRealmSettings.PKC_JWKSET_PATH);
    }

    public static boolean isConfiguredJwkSetPkc(JwtRealm realm) {
        return Strings.hasText(getJwkSetPath(realm));
    }

    public static URI getJwkSetPathUri(JwtRealm realm) {
        final String jwkSetPath = getJwkSetPath(realm);
        return JwtUtil.parseHttpsUri(jwkSetPath);
    }

    public static String getAllowedIssuer(JwtRealm realm) {
        return realm.getConfig().getSetting(JwtRealmSettings.ALLOWED_ISSUER);
    }

    public static List<String> getAllowedAudiences(JwtRealm realm) {
        return realm.getConfig().getSetting(JwtRealmSettings.ALLOWED_AUDIENCES);
    }

    public static boolean shouldPopulateUserMetadata(JwtRealm realm) {
        return realm.getConfig().getSetting(JwtRealmSettings.POPULATE_USER_METADATA);
    }

    public static String getPrincipalClaimName(JwtRealm realm) {
        return getClaimNameFor(realm, JwtRealmSettings.CLAIMS_PRINCIPAL);
    }

    public static String getGroupsClaimName(JwtRealm realm) {
        return getClaimNameFor(realm, JwtRealmSettings.CLAIMS_GROUPS);
    }

    public static JwtRealmSettings.ClientAuthenticationType getClientAuthenticationType(JwtRealm realm) {
        return realm.getConfig().getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_TYPE);
    }

    public static SecureString getClientAuthenticationSharedSecret(JwtRealm realm) {
        final SecureString sharedSecret = realm.getConfig().getSetting(JwtRealmSettings.CLIENT_AUTHENTICATION_SHARED_SECRET);
        return Strings.hasText(sharedSecret) ? sharedSecret : null; // convert "" to null
    }

    public static List<String> getAllowedJwksAlgsHmac(JwtRealm realm) {
        return getAllowedJwksAlgs(realm).stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_HMAC::contains).toList();
    }

    public static List<String> getAllowedJwksAlgsPkc(JwtRealm realm) {
        return getAllowedJwksAlgs(realm).stream().filter(JwtRealmSettings.SUPPORTED_SIGNATURE_ALGORITHMS_PKC::contains).toList();
    }

    public static JwkSetLoader.JwksAlgs getJwksAlgsHmac(JwtRealm realm) {
        return realm.getJwtAuthenticator().getJwtSignatureValidator().getAllJwksAlgs().v1();
    }

    public static JwkSetLoader.JwksAlgs getJwksAlgsPkc(JwtRealm realm) {
        return realm.getJwtAuthenticator().getJwtSignatureValidator().getAllJwksAlgs().v2();
    }

    private static String getClaimNameFor(JwtRealm realm, ClaimSetting claimSetting) {
        final Setting.AffixSetting<String> setting = claimSetting.getClaim();
        if (realm.getConfig().hasSetting(setting)) {
            return realm.getConfig().getSetting(setting);
        } else {
            return null;
        }
    }

    private static List<String> getAllowedJwksAlgs(JwtRealm realm) {
        return realm.getConfig().getSetting(JwtRealmSettings.ALLOWED_SIGNATURE_ALGORITHMS);
    }
}
