/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * TODO REALM-SETTINGS[TIM] This java doc is completely wrong now
 * <p>
 * Configures the {@link Setting#groupSetting(String, Consumer, Setting.Property...) group setting} for security
 * {@link Realm realms}, with validation according to the realm type.
 * <p>
 * The allowable settings for a given realm are dependent on the {@link Realm#type() realm type}, so it is not possible
 * to simply provide a list of {@link Setting} objects and rely on the global setting vacomlidation (e.g. A custom realm-type might
 * define a setting with the same logical key as an internal realm-type, but a different data type).
 * </p> <p>
 * Instead, realm configuration relies on the <code>validator</code> parameter to
 * {@link Setting#groupSetting(String, Consumer, Setting.Property...)} in order to validate each realm in a way that respects the
 * declared <code>type</code>.
 * Internally, this validation delegates to {@link AbstractScopedSettings#validate(Settings, boolean)} so that validation is reasonably
 * aligned
 * with the way we validate settings globally.
 * </p>
 * <p>
 */
public class RealmSettings {

    public static final String PREFIX = "xpack.security.authc.realms.";

    public static final Function<String, Setting.AffixSetting<Boolean>> ENABLED_SETTING = affixSetting("enabled",
            key -> Setting.boolSetting(key, true, Setting.Property.NodeScope));
    public static final Function<String, Setting.AffixSetting<Integer>> ORDER_SETTING = affixSetting("order",
            key -> Setting.intSetting(key, Integer.MAX_VALUE, Setting.Property.NodeScope));

    public static String realmSettingPrefix(String type) {
        return PREFIX + type + ".";
    }

    public static String realmSettingPrefix(RealmConfig.RealmIdentifier identifier) {
        return realmSettingPrefix(identifier.type) + identifier.name + ".";
    }

    public static Setting.AffixSetting<String> simpleString(String realmType, String suffix, Setting.Property... properties) {
        return Setting.affixKeySetting(realmSettingPrefix(realmType), suffix, key -> Setting.simpleString(key, properties));
    }

    public static <T> Function<String, Setting.AffixSetting<T>> affixSetting(String suffix, Function<String, Setting<T>> delegateFactory) {
        return realmType -> Setting.affixKeySetting(realmSettingPrefix(realmType), suffix, delegateFactory);
    }

    /**
     * Extract the child {@link Settings} for the {@link #PREFIX realms prefix}.
     * The top level names in the returned <code>Settings</code> will be the types of the configured realms.
     */
    public static Settings get(Settings settings) {
        return settings.getByPrefix(RealmSettings.PREFIX);
    }

    /**
     * Extracts the realm settings from a global settings object.
     * Returns a Map of realm-id to realm-settings.
     */
    public static Map<RealmConfig.RealmIdentifier, Settings> getRealmSettings(Settings globalSettings) {
        Settings settingsByType = RealmSettings.get(globalSettings);
        return settingsByType.names().stream()
                .flatMap(type -> {
                    final Settings settingsByName = settingsByType.getAsSettings(type);
                    return settingsByName.names().stream().map(name -> {
                        final RealmConfig.RealmIdentifier id = new RealmConfig.RealmIdentifier(type, name);
                        final Settings realmSettings = settingsByName.getAsSettings(name);
                        return new Tuple<>(id, realmSettings);
                    });
                })
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));
    }

    public static String getFullSettingKey(String realmName, Setting.AffixSetting<?> setting) {
        return setting.getConcreteSettingForNamespace(realmName).getKey();
    }

    public static String getFullSettingKey(RealmConfig realm, Setting.AffixSetting<?> setting) {
        return setting.getConcreteSettingForNamespace(realm.name()).getKey();
    }

    public static <T> String getFullSettingKey(RealmConfig.RealmIdentifier realmId, Function<String, Setting.AffixSetting<T>> setting) {
        return getFullSettingKey(realmId.name, setting.apply(realmId.type));
    }

    public static <T> String getFullSettingKey(RealmConfig realm, Function<String, Setting.AffixSetting<T>> setting) {
        return getFullSettingKey(realm.identifier, setting);
    }

    private RealmSettings() {
    }

    public static List<Setting.AffixSetting<?>> getStandardSettings(String realmType) {
        return Arrays.asList(ENABLED_SETTING.apply(realmType), ORDER_SETTING.apply(realmType));

    }
}
