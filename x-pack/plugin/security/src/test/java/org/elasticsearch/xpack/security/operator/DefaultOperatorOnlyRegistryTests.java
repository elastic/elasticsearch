/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.transport.filter.IPFilter.HTTP_FILTER_ALLOW_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.HTTP_FILTER_DENY_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.IP_FILTER_ENABLED_HTTP_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.IP_FILTER_ENABLED_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.PROFILE_FILTER_ALLOW_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.PROFILE_FILTER_DENY_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.TRANSPORT_FILTER_ALLOW_SETTING;
import static org.elasticsearch.xpack.security.transport.filter.IPFilter.TRANSPORT_FILTER_DENY_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultOperatorOnlyRegistryTests extends ESTestCase {

    private static final Set<Setting<?>> DYNAMIC_SETTINGS = ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream()
        .filter(Setting::isDynamic)
        .filter(setting -> false == setting.isOperatorOnly())
        .collect(Collectors.toSet());
    private static final Set<Setting<?>> IP_FILTER_SETTINGS = Set.of(
        IP_FILTER_ENABLED_HTTP_SETTING,
        IP_FILTER_ENABLED_SETTING,
        TRANSPORT_FILTER_ALLOW_SETTING,
        TRANSPORT_FILTER_DENY_SETTING,
        PROFILE_FILTER_DENY_SETTING,
        PROFILE_FILTER_ALLOW_SETTING,
        HTTP_FILTER_ALLOW_SETTING,
        HTTP_FILTER_DENY_SETTING
    );

    private DefaultOperatorOnlyRegistry operatorOnlyRegistry;

    @Before
    public void init() {
        final Set<Setting<?>> settingsSet = new HashSet<>(IP_FILTER_SETTINGS);
        settingsSet.addAll(DYNAMIC_SETTINGS);
        operatorOnlyRegistry = new DefaultOperatorOnlyRegistry(new ClusterSettings(Settings.EMPTY, settingsSet));
    }

    public void testSimpleOperatorOnlyApi() {
        for (final String actionName : DefaultOperatorOnlyRegistry.SIMPLE_ACTIONS) {
            final DefaultOperatorOnlyRegistry.OperatorPrivilegesViolation violation = operatorOnlyRegistry.check(actionName, null);
            assertNotNull(violation);
            assertThat(violation.message(), containsString("action [" + actionName + "]"));
        }
    }

    public void testNonOperatorOnlyApi() {
        final String actionName = randomValueOtherThanMany(
            DefaultOperatorOnlyRegistry.SIMPLE_ACTIONS::contains,
            () -> randomAlphaOfLengthBetween(10, 40)
        );
        assertNull(operatorOnlyRegistry.check(actionName, null));
    }

    public void testOperatorOnlySettings() {
        final ClusterUpdateSettingsRequest request;
        final Setting<?> transientSetting;
        final Setting<?> persistentSetting;
        final DefaultOperatorOnlyRegistry.OperatorPrivilegesViolation violation;

        switch (randomIntBetween(0, 3)) {
            case 0 -> {
                transientSetting = convertToConcreteSettingIfNecessary(randomFrom(IP_FILTER_SETTINGS));
                persistentSetting = convertToConcreteSettingIfNecessary(
                    randomValueOtherThan(transientSetting, () -> randomFrom(IP_FILTER_SETTINGS))
                );
                request = prepareClusterUpdateSettingsRequest(transientSetting, persistentSetting);
                violation = operatorOnlyRegistry.check(ClusterUpdateSettingsAction.NAME, request);
                assertThat(
                    violation.message(),
                    containsString(Strings.format("settings [%s,%s]", transientSetting.getKey(), persistentSetting.getKey()))
                );
            }
            case 1 -> {
                transientSetting = convertToConcreteSettingIfNecessary(randomFrom(IP_FILTER_SETTINGS));
                persistentSetting = convertToConcreteSettingIfNecessary(randomFrom(DYNAMIC_SETTINGS));
                request = prepareClusterUpdateSettingsRequest(transientSetting, persistentSetting);
                violation = operatorOnlyRegistry.check(ClusterUpdateSettingsAction.NAME, request);
                assertThat(violation.message(), containsString(Strings.format("setting [%s]", transientSetting.getKey())));
            }
            case 2 -> {
                transientSetting = convertToConcreteSettingIfNecessary(randomFrom(DYNAMIC_SETTINGS));
                persistentSetting = convertToConcreteSettingIfNecessary(randomFrom(IP_FILTER_SETTINGS));
                request = prepareClusterUpdateSettingsRequest(transientSetting, persistentSetting);
                violation = operatorOnlyRegistry.check(ClusterUpdateSettingsAction.NAME, request);
                assertThat(violation.message(), containsString(Strings.format("setting [%s]", persistentSetting.getKey())));
            }
            case 3 -> {
                transientSetting = convertToConcreteSettingIfNecessary(randomFrom(DYNAMIC_SETTINGS));
                persistentSetting = convertToConcreteSettingIfNecessary(randomFrom(DYNAMIC_SETTINGS));
                request = prepareClusterUpdateSettingsRequest(transientSetting, persistentSetting);
                assertNull(operatorOnlyRegistry.check(ClusterUpdateSettingsAction.NAME, request));
            }
        }

    }

    private Setting<?> convertToConcreteSettingIfNecessary(Setting<?> setting) {
        if (setting instanceof Setting.AffixSetting) {
            return ((Setting.AffixSetting<?>) setting).getConcreteSettingForNamespace(randomAlphaOfLengthBetween(4, 8));
        } else {
            return setting;
        }
    }

    private ClusterUpdateSettingsRequest prepareClusterUpdateSettingsRequest(Setting<?> transientSetting, Setting<?> persistentSetting) {
        final ClusterUpdateSettingsRequest request = mock(ClusterUpdateSettingsRequest.class);
        when(request.transientSettings()).thenReturn(Settings.builder().put(transientSetting.getKey(), "null").build());
        when(request.persistentSettings()).thenReturn(Settings.builder().put(persistentSetting.getKey(), "null").build());
        return request;
    }

}
