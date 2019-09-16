/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides a number of utility methods for interacting with {@link Settings} and {@link Setting} inside a {@link Realm}.
 * Settings for realms use an {@link Setting#affixKeySetting(String, String, Function, Setting.AffixSetting[]) affix} style,
 * where the <em>type</em> of the realm is part of the prefix, and name of the realm is the variable portion
 * (That is to set the order in a file realm named "file1", then full setting key would be
 * {@code xpack.security.authc.realms.file.file1.order}.
 * This class provides some convenience methods for defining and retrieving such settings.
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
        return realmSettingPrefix(identifier.getType()) + identifier.getName() + ".";
    }

    public static String realmSslPrefix(RealmConfig.RealmIdentifier identifier) {
        return realmSettingPrefix(identifier) + "ssl.";
    }

    /**
     * Create a {@link Setting#simpleString(String, Setting.Property...) simple string} {@link Setting} object for a realm of
     * with the provided type and setting suffix.
     * @param realmType The type of the realm, used within the setting prefix
     * @param suffix The suffix of the setting (everything following the realm name in the affix setting)
     * @param properties And properties to apply to the setting
     */
    public static Setting.AffixSetting<String> simpleString(String realmType, String suffix, Setting.Property... properties) {
        return Setting.affixKeySetting(realmSettingPrefix(realmType), suffix, key -> Setting.simpleString(key, properties));
    }

    /**
     * Create a {@link SecureSetting#secureString secure string} {@link Setting} object of a realm of
     * with the provided type and setting suffix.
     *
     * @param realmType The type of the realm, used within the setting prefix
     * @param suffix    The suffix of the setting (everything following the realm name in the affix setting)
     */
    public static Setting.AffixSetting<SecureString> secureString(String realmType, String suffix) {
        return Setting.affixKeySetting(realmSettingPrefix(realmType), suffix, key -> SecureSetting.secureString(key, null));
    }

    /**
     * Create a {@link Function} that acts as a factory an {@link org.elasticsearch.common.settings.Setting.AffixSetting}.
     * The {@code Function} takes the <em>realm-type</em> as an argument.
     * @param suffix The suffix of the setting (everything following the realm name in the affix setting)
     * @param delegateFactory A factory to produce the concrete setting.
     *                       See {@link Setting#affixKeySetting(Setting.AffixKey, Function, Setting.AffixSetting[])}
     */
    public static <T> Function<String, Setting.AffixSetting<T>> affixSetting(String suffix, Function<String, Setting<T>> delegateFactory) {
        return realmType -> Setting.affixKeySetting(realmSettingPrefix(realmType), suffix, delegateFactory);
    }

    /**
     * Extracts the realm settings from a global settings object.
     * Returns a Map of realm-id to realm-settings.
     */
    public static Map<RealmConfig.RealmIdentifier, Settings> getRealmSettings(Settings globalSettings) {
        Settings settingsByType = globalSettings.getByPrefix(RealmSettings.PREFIX);
        return settingsByType.names().stream()
                .flatMap(type -> {
                    final Settings settingsByName = settingsByType.getAsSettings(type);
                    return settingsByName.names().stream().map(name -> {
                        final RealmConfig.RealmIdentifier id = new RealmConfig.RealmIdentifier(type, name);
                        final Settings realmSettings = settingsByName.getAsSettings(name);
                        verifyRealmSettings(id, realmSettings);
                        return new Tuple<>(id, realmSettings);
                    });
                })
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));
    }

    /**
     * Performs any necessary verifications on a realms settings that are not automatically applied by Settings validation infrastructure.
     */
    private static void verifyRealmSettings(RealmConfig.RealmIdentifier identifier, Settings realmSettings) {
        final Settings nonSecureSettings = Settings.builder().put(realmSettings, false).build();
        if (nonSecureSettings.isEmpty()) {
            final String prefix = realmSettingPrefix(identifier);
            throw new SettingsException(
                "found settings for the realm [{}] (with type [{}]) in the secure settings (elasticsearch.keystore)," +
                    " but this realm does not have any settings in elasticsearch.yml." +
                    " Please remove these settings from the keystore, or update their names to match one of the realms that are" +
                    " defined in elasticsearch.yml - [{}]",
                identifier.getName(), identifier.getType(),
                realmSettings.keySet().stream().map(k -> prefix + k).collect(Collectors.joining(","))
            );
        }
    }

    public static String getFullSettingKey(String realmName, Setting.AffixSetting<?> setting) {
        return setting.getConcreteSettingForNamespace(realmName).getKey();
    }

    public static String getFullSettingKey(RealmConfig realm, Setting.AffixSetting<?> setting) {
        return setting.getConcreteSettingForNamespace(realm.name()).getKey();
    }

    public static <T> String getFullSettingKey(RealmConfig.RealmIdentifier realmId, Function<String, Setting.AffixSetting<T>> setting) {
        return getFullSettingKey(realmId.getName(), setting.apply(realmId.getType()));
    }

    public static <T> String getFullSettingKey(RealmConfig realm, Function<String, Setting.AffixSetting<T>> setting) {
        return getFullSettingKey(realm.identifier, setting);
    }

    public static List<Setting.AffixSetting<?>> getStandardSettings(String realmType) {
        return Arrays.asList(ENABLED_SETTING.apply(realmType), ORDER_SETTING.apply(realmType));
    }

    private RealmSettings() {
    }

}
