/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.settings;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.ALLOWED_SETTING_VALIDATORS;
import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.MAIN_INDEX_NAME;
import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.PROFILES_INDEX_NAME;
import static org.elasticsearch.xpack.core.security.action.settings.UpdateSecuritySettingsAction.TOKENS_INDEX_NAME;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class UpdateSecuritySettingsActionTests extends ESTestCase {

    static final Map<String, Supplier<String>> ALLOWED_SETTING_GENERATORS = Map.of(
        IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
        () -> randomAlphaOfLength(5), // no additional validation
        IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS,
        () -> randomAlphaOfLength(5), // no additional validation
        DataTier.TIER_PREFERENCE,
        () -> randomFrom(DataTier.DATA_CONTENT, DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD)
    );

    public void testValidateSettingsEmpty() {
        var req = new UpdateSecuritySettingsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
        );
        var ex = req.validate();
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), containsString("No settings given to update"));
        assertThat(ex.validationErrors(), hasSize(1));
    }

    public void testAllowedSettingsOk() {
        Map<String, Object> allAllowedSettingsMap = new HashMap<>();
        for (String allowedSetting : ALLOWED_SETTING_VALIDATORS.keySet()) {
            String settingValue = ALLOWED_SETTING_GENERATORS.get(allowedSetting).get();
            Map<String, Object> allowedSettingMap = Map.of(allowedSetting, settingValue);
            allAllowedSettingsMap.put(allowedSetting, settingValue);
            var req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                allowedSettingMap,
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            assertThat(req.validate(), nullValue());

            req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                Collections.emptyMap(),
                allowedSettingMap,
                Collections.emptyMap()
            );
            assertThat(req.validate(), nullValue());

            req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                Collections.emptyMap(),
                Collections.emptyMap(),
                allowedSettingMap
            );
            assertThat(req.validate(), nullValue());
        }

        var req = new UpdateSecuritySettingsAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            allAllowedSettingsMap,
            allAllowedSettingsMap,
            allAllowedSettingsMap
        );
        assertThat(req.validate(), nullValue());
    }

    public void testDisallowedSettingsFailsValidation() {
        String disallowedSetting = "index."
            + randomValueOtherThanMany((value) -> ALLOWED_SETTING_VALIDATORS.containsKey("index." + value), () -> randomAlphaOfLength(5));
        Map<String, Object> disallowedSettingMap = Map.of(disallowedSetting, randomAlphaOfLength(5));
        String validSetting = randomFrom(ALLOWED_SETTING_VALIDATORS.keySet());
        Map<String, Object> validOrEmptySettingMap = randomFrom(
            Collections.emptyMap(),
            Map.of(validSetting, ALLOWED_SETTING_GENERATORS.get(validSetting).get())
        );
        {
            var req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                validOrEmptySettingMap,
                disallowedSettingMap,
                validOrEmptySettingMap
            );
            List<String> errors = req.validate().validationErrors();
            assertThat(errors, hasSize(1));
            for (String errorMsg : errors) {
                assertThat(
                    errorMsg,
                    matchesRegex(
                        "illegal setting for index \\["
                            + Pattern.quote(TOKENS_INDEX_NAME)
                            + "\\]: \\["
                            + disallowedSetting
                            + "\\], this setting may not be configured. Only the following settings may be configured for that index.*"
                    )
                );
            }
        }

        {
            var req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                disallowedSettingMap,
                validOrEmptySettingMap,
                disallowedSettingMap
            );
            List<String> errors = req.validate().validationErrors();
            assertThat(errors, hasSize(2));
            for (String errorMsg : errors) {
                assertThat(
                    errorMsg,
                    matchesRegex(
                        "illegal setting for index \\[("
                            + Pattern.quote(MAIN_INDEX_NAME)
                            + "|"
                            + Pattern.quote(PROFILES_INDEX_NAME)
                            + ")\\]: \\["
                            + disallowedSetting
                            + "\\], this setting may not be configured. Only the following settings may be configured for that index.*"
                    )
                );
            }
        }

        {
            var req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                disallowedSettingMap,
                disallowedSettingMap,
                disallowedSettingMap
            );
            List<String> errors = req.validate().validationErrors();
            assertThat(errors, hasSize(3));
            for (String errorMsg : errors) {
                assertThat(
                    errorMsg,
                    matchesRegex(
                        "illegal setting for index \\[("
                            + Pattern.quote(MAIN_INDEX_NAME)
                            + "|"
                            + Pattern.quote(TOKENS_INDEX_NAME)
                            + "|"
                            + Pattern.quote(PROFILES_INDEX_NAME)
                            + ")\\]: \\["
                            + disallowedSetting
                            + "\\], this setting may not be configured. Only the following settings may be configured for that index.*"
                    )
                );
            }
        }
    }

    public void testSettingValuesAreValidated() {
        Map<String, Object> forbiddenSettingsMap = Map.of(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN);
        String badTier = randomAlphaOfLength(5);
        Map<String, Object> badSettingsMap = Map.of(DataTier.TIER_PREFERENCE, badTier);
        Map<String, Object> allowedSettingMap = Map.of(
            DataTier.TIER_PREFERENCE,
            randomFrom(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_CONTENT, DataTier.DATA_COLD)
        );
        {
            var req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                allowedSettingMap,
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            assertThat(req.validate(), nullValue());
        }

        {
            var req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                forbiddenSettingsMap,
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            ActionRequestValidationException exception = req.validate();
            assertThat(exception, notNullValue());
            assertThat(exception.validationErrors(), hasSize(1));
            assertThat(
                exception.validationErrors().get(0),
                containsString("disallowed data tiers [" + DataTier.DATA_FROZEN + "] found, allowed tiers are ")
            );
        }

        {
            var req = new UpdateSecuritySettingsAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                badSettingsMap,
                Collections.emptyMap(),
                Collections.emptyMap()
            );
            var exception = req.validate();
            assertThat(exception, notNullValue());
            assertThat(exception.validationErrors(), hasSize(1));
            assertThat(
                exception.validationErrors().get(0),
                containsString("disallowed data tiers [" + badTier + "] found, allowed tiers are ")
            );
        }
    }
}
