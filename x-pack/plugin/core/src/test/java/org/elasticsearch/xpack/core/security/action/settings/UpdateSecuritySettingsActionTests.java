/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.settings;

import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.ALLOWED_SETTING_KEYS;
import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.MAIN_INDEX_NAME;
import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.PROFILES_INDEX_NAME;
import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.TOKENS_INDEX_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSecuritySettingsActionTests extends ESTestCase {

    public void testValidateSettingsEmpty() {
        var req = new UpdateSecuritySettingsAction.Request(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
        var ex = req.validate();
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), containsString("No settings given to update"));
        assertThat(ex.validationErrors(), hasSize(1));
    }

    public void testAllowedSettingsOk() {
        Map<String, Object> allAllowedSettingsMap = new HashMap<>();
        for (String allowedSetting : ALLOWED_SETTING_KEYS) {
            Map<String, Object> allowedSettingMap = Map.of(allowedSetting, randomAlphaOfLength(5));
            allAllowedSettingsMap.put(allowedSetting, randomAlphaOfLength(5));
            var req = new UpdateSecuritySettingsAction.Request(allowedSettingMap, Collections.emptyMap(), Collections.emptyMap());
            assertThat(req.validate(), nullValue());

            req = new UpdateSecuritySettingsAction.Request(Collections.emptyMap(), allowedSettingMap, Collections.emptyMap());
            assertThat(req.validate(), nullValue());

            req = new UpdateSecuritySettingsAction.Request(Collections.emptyMap(), Collections.emptyMap(), allowedSettingMap);
            assertThat(req.validate(), nullValue());
        }

        var req = new UpdateSecuritySettingsAction.Request(allAllowedSettingsMap, allAllowedSettingsMap, allAllowedSettingsMap);
        assertThat(req.validate(), nullValue());
    }

    public void testDisallowedSettingsFailsValidation() {
        String disallowedSetting = "index."
            + randomValueOtherThanMany((value) -> ALLOWED_SETTING_KEYS.contains("index." + value), () -> randomAlphaOfLength(5));
        Map<String, Object> disallowedSettingMap = Map.of(disallowedSetting, randomAlphaOfLength(5));
        Map<String, Object> validOrEmptySettingMap = randomFrom(
            Collections.emptyMap(),
            Map.of(randomFrom(ALLOWED_SETTING_KEYS), randomAlphaOfLength(5))
        );
        {
            var req = new UpdateSecuritySettingsAction.Request(validOrEmptySettingMap, disallowedSettingMap, validOrEmptySettingMap);
            List<String> errors = req.validate().validationErrors();
            assertThat(errors, hasSize(1));
            for (String errorMsg : errors) {
                assertThat(
                    errorMsg,
                    matchesRegex(
                        "illegal settings for index \\["
                            + Pattern.quote(TOKENS_INDEX_NAME)
                            + "\\]: \\["
                            + disallowedSetting
                            + "\\], these settings may not be configured. Only the following settings may be configured for that index.*"
                    )
                );
            }
        }

        {
            var req = new UpdateSecuritySettingsAction.Request(disallowedSettingMap, validOrEmptySettingMap, disallowedSettingMap);
            List<String> errors = req.validate().validationErrors();
            assertThat(errors, hasSize(2));
            for (String errorMsg : errors) {
                assertThat(
                    errorMsg,
                    matchesRegex(
                        "illegal settings for index \\[("
                            + Pattern.quote(MAIN_INDEX_NAME)
                            + "|"
                            + Pattern.quote(PROFILES_INDEX_NAME)
                            + ")\\]: \\["
                            + disallowedSetting
                            + "\\], these settings may not be configured. Only the following settings may be configured for that index.*"
                    )
                );
            }
        }

        {
            var req = new UpdateSecuritySettingsAction.Request(disallowedSettingMap, disallowedSettingMap, disallowedSettingMap);
            List<String> errors = req.validate().validationErrors();
            assertThat(errors, hasSize(3));
            for (String errorMsg : errors) {
                assertThat(
                    errorMsg,
                    matchesRegex(
                        "illegal settings for index \\[("
                            + Pattern.quote(MAIN_INDEX_NAME)
                            + "|"
                            + Pattern.quote(TOKENS_INDEX_NAME)
                            + "|"
                            + Pattern.quote(PROFILES_INDEX_NAME)
                            + ")\\]: \\["
                            + disallowedSetting
                            + "\\], these settings may not be configured. Only the following settings may be configured for that index.*"
                    )
                );
            }
        }
    }

}
