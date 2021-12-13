/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Arrays;
import java.util.Collection;

/**
 * The OIDC and JWT realms offer a number of settings that rely on claim values that are populated by the JWT issuer. In the OIDC realm,
 * the issuer is the OIDC OP, and claims can be added from the User Info response too.
 * Each claim has 2 settings:
 * <ul>
 * <li>The name of the JWT claim to use</li>
 * <li>An optional java pattern (regex) to apply to that claim value in order to extract the substring that should be used.</li>
 * </ul>
 * For example, the Elasticsearch User Principal could be configured to come from the OpenID Connect standard claim "email",
 * and extract only the local-part of the user's email address (i.e. the name before the '@').
 * This class encapsulates those 2 settings.
 */
public final class ClaimSetting {
    public static final String CLAIMS_PREFIX = "claims.";
    public static final String CLAIM_PATTERNS_PREFIX = "claim_patterns.";

    private final Setting.AffixSetting<String> claim;
    private final Setting.AffixSetting<String> pattern;

    // type should be one of OpenIdConnectRealmSettings.TYPE="oidc" or JwtRealmSettings.TYPE="jwt"
    public ClaimSetting(String type, String name) {
        claim = RealmSettings.simpleString(type, CLAIMS_PREFIX + name, Setting.Property.NodeScope);
        pattern = RealmSettings.simpleString(type, CLAIM_PATTERNS_PREFIX + name, Setting.Property.NodeScope);
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
