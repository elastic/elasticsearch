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

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * JWT-related realms extract user data from JWTs. Some realms can also look up extra user data elsewhere;
 * OIDC realms can query an OIDC OP User Info endpoint to retrieve extra user data not specified in the JWT.
 *
 * Each piece of extractable user data is configurable via two settings. This class encapsulates two settings per claim.
 * <ul>
 * <li>The claim name.</li>
 * <li>An optional java pattern (regex) to extract a substring from the claim value.</li>
 * </ul>
 *
 * For example, an Elasticsearch User Principal could be configured to use claims like 'sub', 'name', 'email', or 'dn'.
 * <ul>
 * <li>In the case of 'sub' and 'name', no regex would be needed.</li>
 * <li>In the case of 'email', a regex could be used to extract just the username before the '@'.</li>
 * <li>In the case of 'dn', a regex could be used to extract just the 'cn' or 'uid' AVA value in a Distinguished Name.</li>
 * </ul>
 */
public final class ClaimSetting {
    public static final String CLAIMS_PREFIX = "claims.";
    public static final String CLAIM_PATTERNS_PREFIX = "claim_patterns.";

    private final Setting.AffixSetting<String> claim;
    private final Setting.AffixSetting<String> pattern;

    public ClaimSetting(final String realmType, final String settingName) {
        if ((realmType == null) || realmType.isEmpty()) {
            throw new IllegalArgumentException("Invalid realm type [" + realmType + "].");
        } else if ((settingName == null) || settingName.isEmpty()) {
            throw new IllegalArgumentException("Invalid claim setting name [" + settingName + "].");
        }
        this.claim = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(realmType),
            CLAIMS_PREFIX + settingName,
            key -> Setting.simpleString(key, value -> verifyName(key, value), Setting.Property.NodeScope)
        );
        this.pattern = Setting.affixKeySetting(
            RealmSettings.realmSettingPrefix(realmType),
            CLAIM_PATTERNS_PREFIX + settingName,
            key -> Setting.simpleString(key, value -> verifyPattern(key, value), Setting.Property.NodeScope)
        );
    }

    public Collection<Setting.AffixSetting<?>> settings() {
        return List.of(this.getClaim(), this.getPattern());
    }

    public String name(final RealmConfig realmConfig) {
        return this.getClaim().getConcreteSettingForNamespace(realmConfig.name()).getKey();
    }

    public Setting.AffixSetting<String> getClaim() {
        return this.claim;
    }

    public Setting.AffixSetting<String> getPattern() {
        return this.pattern;
    }

    private static void verifyName(final String key, final String value) {
        if ((value == null) || (value.isEmpty())) {
            throw new IllegalArgumentException("Invalid null or empty claim name for [" + key + "].");
        }
    }

    private static void verifyPattern(final String key, final String value) {
        if ((value != null) && (value.isEmpty() == false)) {
            try {
                Pattern.compile(value);
            } catch (PatternSyntaxException pse) {
                throw new IllegalArgumentException("Invalid claim value regex pattern for [" + key + "].");
            }
        }
    }
}
