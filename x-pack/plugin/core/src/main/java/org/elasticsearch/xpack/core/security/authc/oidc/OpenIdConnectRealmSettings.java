/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.oidc;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;


public class OpenIdConnectRealmSettings {

    private OpenIdConnectRealmSettings() {
    }

    public static final String TYPE = "oidc";

    public static final Setting.AffixSetting<String> RP_CLIENT_ID
        = RealmSettings.simpleString(TYPE, "rp.client_id", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<SecureString> RP_CLIENT_SECRET
        = RealmSettings.secureString(TYPE, "rp.client_secret");
    public static final Setting.AffixSetting<String> RP_REDIRECT_URI
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "rp.redirect_uri",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> RP_RESPONSE_TYPE
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "rp.response_type",
        key -> Setting.simpleString(key, v -> {
            List<String> responseTypes = Arrays.asList("code", "id_token");
            if (responseTypes.contains(v) == false) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Allowed values are " + responseTypes + "");
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> RP_SIGNATURE_VERIFICATION_ALGORITHM
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "rp.signature_verification_algorithm",
        key -> new Setting<>(key, "RS256", Function.identity(), v -> {
            List<String> sigAlgo = Arrays.asList("HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "ES256", "ES384",
                "ES512", "PS256", "PS384", "PS512");
            if (sigAlgo.contains(v) == false) {
                throw new IllegalArgumentException(
                    "Invalid value [" + v + "] for [" + key + "]. Allowed values are " + sigAlgo + "}]");
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<List<String>> RP_REQUESTED_SCOPES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "rp.requested_scopes",
        key -> Setting.listSetting(key, Collections.singletonList("openid"), Function.identity(), Setting.Property.NodeScope));

    public static final Setting.AffixSetting<String> OP_NAME
        = RealmSettings.simpleString(TYPE, "op.name", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> OP_AUTHORIZATION_ENDPOINT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "op.authorization_endpoint",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> OP_TOKEN_ENDPOINT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "op.token_endpoint",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> OP_USERINFO_ENDPOINT
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "op.token_endpoint",
        key -> Setting.simpleString(key, v -> {
            try {
                new URI(v);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URI.", e);
            }
        }, Setting.Property.NodeScope));
    public static final Setting.AffixSetting<String> OP_ISSUER
        = RealmSettings.simpleString(TYPE, "op.issuer", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> OP_JWKSET_URL
        = Setting.affixKeySetting(RealmSettings.realmSettingPrefix(TYPE), "op.jwkset_url",
        key -> Setting.simpleString(key, v -> {
            try {
                new URL(v);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid value [" + v + "] for [" + key + "]. Not a valid URL.", e);
            }
        }, Setting.Property.NodeScope));

    public static final Setting.AffixSetting<Boolean> POPULATE_USER_METADATA = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE), "populate_user_metadata",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope));

    public static final ClaimSetting PRINCIPAL_CLAIM = new ClaimSetting("principal");
    public static final ClaimSetting GROUPS_CLAIM = new ClaimSetting("groups");
    public static final ClaimSetting NAME_CLAIM = new ClaimSetting("name");
    public static final ClaimSetting DN_CLAIM = new ClaimSetting("dn");
    public static final ClaimSetting MAIL_CLAIM = new ClaimSetting("mail");

    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet(
            RP_CLIENT_ID, RP_REDIRECT_URI, RP_RESPONSE_TYPE, RP_REQUESTED_SCOPES, RP_CLIENT_SECRET, RP_SIGNATURE_VERIFICATION_ALGORITHM,
            OP_NAME, OP_AUTHORIZATION_ENDPOINT, OP_TOKEN_ENDPOINT, OP_USERINFO_ENDPOINT, OP_ISSUER, OP_JWKSET_URL);
        set.addAll(DelegatedAuthorizationSettings.getSettings(TYPE));
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        set.addAll(SSLConfigurationSettings.getRealmSettings(TYPE));
        set.addAll(PRINCIPAL_CLAIM.settings());
        set.addAll(GROUPS_CLAIM.settings());
        set.addAll(DN_CLAIM.settings());
        set.addAll(NAME_CLAIM.settings());
        set.addAll(MAIL_CLAIM.settings());
        return set;
    }


    /**
     * The OIDC realm offers a number of settings that rely on claim values that are populated by the OP in the ID Token or the User Info
     * response.
     * Each claim has 2 settings:
     * <ul>
     * <li>The name of the OpenID Connect claim to use</li>
     * <li>An optional java pattern (regex) to apply to that claim value in order to extract the substring that should be used.</li>
     * </ul>
     * For example, the Elasticsearch User Principal could be configured to come from the OpenID Connect standard claim "email",
     * and extract only the local-port of the user's email address (i.e. the name before the '@').
     * This class encapsulates those 2 settings.
     */
    public static final class ClaimSetting {
        public static final String CLAIMS_PREFIX = "claims.";
        public static final String CLAIM_PATTERNS_PREFIX = "claim_patterns.";

        private final Setting.AffixSetting<String> claim;
        private final Setting.AffixSetting<String> pattern;

        public ClaimSetting(String name) {
            claim = RealmSettings.simpleString(TYPE, CLAIMS_PREFIX + name, Setting.Property.NodeScope);
            pattern = RealmSettings.simpleString(TYPE, CLAIM_PATTERNS_PREFIX + name, Setting.Property.NodeScope);
        }

        public Collection<Setting.AffixSetting<?>> settings() {
            return Arrays.asList(getClaim(), getPattern());
        }

        public String name(RealmConfig config) {
            return getClaim().getConcreteSettingForNamespace(config.name()).getKey();
        }

        public Setting.AffixSetting<String> getClaim() {
            return claim;
        }

        public Setting.AffixSetting<String> getPattern() {
            return pattern;
        }
    }
}
