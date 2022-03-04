/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import com.nimbusds.jwt.JWTClaimsSet;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.ClaimSetting;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class for parsing JWT claims.
 */
public final class ClaimParser {
    private final String setting;
    private final String claimName;
    private final String regexPattern;
    private final Function<JWTClaimsSet, List<String>> parser;

    public ClaimParser(String setting, String claimName, String regexPattern, Function<JWTClaimsSet, List<String>> parser) {
        this.setting = setting;
        this.claimName = claimName;
        this.regexPattern = regexPattern;
        this.parser = parser;
    }

    public String getSetting() {
        return this.setting;
    }

    public String getClaimName() {
        return this.claimName;
    }

    public String getRegexPattern() {
        return this.regexPattern;
    }

    public Function<JWTClaimsSet, List<String>> getParser() {
        return this.parser;
    }

    public List<String> getClaimValues(JWTClaimsSet claims) {
        return parser.apply(claims);
    }

    public String getClaimValue(JWTClaimsSet claims) {
        List<String> claimValues = parser.apply(claims);
        if (claimValues == null || claimValues.isEmpty()) {
            return null;
        } else {
            return claimValues.get(0);
        }
    }

    @Override
    public String toString() {
        if (this.claimName == null) {
            return "No claim for [" + this.setting + "]";
        } else if (this.regexPattern == null) {
            return "Claim [" + this.claimName + "] for [" + this.setting + "]";
        } else {
            return "Claim [" + this.claimName + "] with pattern [" + this.regexPattern + "] for [" + this.setting + "]";
        }
    }

    @SuppressWarnings("unchecked")
    private static Collection<String> parseClaimValues(JWTClaimsSet claimsSet, String claimName, String settingKey) {
        Collection<String> values;
        final Object claimValueObject = claimsSet.getClaim(claimName);
        if (claimValueObject == null) {
            values = List.of();
        } else if (claimValueObject instanceof String) {
            values = List.of((String) claimValueObject);
        } else if (claimValueObject instanceof Collection
            && ((Collection<?>) claimValueObject).stream().allMatch(c -> c instanceof String)) {
                values = (Collection<String>) claimValueObject;
            } else {
                throw new SettingsException("Setting [ " + settingKey + " expects a claim with String or a String Array value");
            }
        return values;
    }

    public static ClaimParser forSetting(Logger logger, ClaimSetting setting, RealmConfig realmConfig, boolean required) {

        if (realmConfig.hasSetting(setting.getClaim())) {
            String claimName = realmConfig.getSetting(setting.getClaim());
            if (realmConfig.hasSetting(setting.getPattern())) {
                Pattern regex = Pattern.compile(realmConfig.getSetting(setting.getPattern()));
                return new ClaimParser(setting.name(realmConfig), claimName, regex.pattern(), claims -> {
                    Collection<String> values = parseClaimValues(
                        claims,
                        claimName,
                        RealmSettings.getFullSettingKey(realmConfig, setting.getClaim())
                    );
                    return values.stream().map(s -> {
                        if (s == null) {
                            logger.debug("Claim [{}] is null", claimName);
                            return null;
                        }
                        final Matcher matcher = regex.matcher(s);
                        if (matcher.find() == false) {
                            logger.debug("Claim [{}] is [{}], which does not match [{}]", claimName, s, regex.pattern());
                            return null;
                        }
                        final String value = matcher.group(1);
                        if (Strings.isNullOrEmpty(value)) {
                            logger.debug("Claim [{}] is [{}], which does match [{}] but group(1) is empty", claimName, s, regex.pattern());
                            return null;
                        }
                        return value;
                    }).filter(Objects::nonNull).toList();
                });
            } else {
                return new ClaimParser(
                    setting.name(realmConfig),
                    claimName,
                    null,
                    claims -> parseClaimValues(claims, claimName, RealmSettings.getFullSettingKey(realmConfig, setting.getClaim())).stream()
                        .filter(Objects::nonNull)
                        .toList()
                );
            }
        } else if (required) {
            throw new SettingsException("Setting [" + RealmSettings.getFullSettingKey(realmConfig, setting.getClaim()) + "] is required");
        } else if (realmConfig.hasSetting(setting.getPattern())) {
            throw new SettingsException(
                "Setting ["
                    + RealmSettings.getFullSettingKey(realmConfig, setting.getPattern())
                    + "] cannot be set unless ["
                    + RealmSettings.getFullSettingKey(realmConfig, setting.getClaim())
                    + "] is also set"
            );
        } else {
            return new ClaimParser(setting.name(realmConfig), null, null, attributes -> List.of());
        }
    }
}
