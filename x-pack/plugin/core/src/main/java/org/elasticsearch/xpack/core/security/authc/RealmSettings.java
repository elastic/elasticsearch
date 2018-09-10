/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.settings.AbstractScopedSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.SecurityExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.isNullOrEmpty;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
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
 * The allowable settings for each realm-type are determined by calls to {@link InternalRealmsSettings#getSettings()} and
 * {@link org.elasticsearch.xpack.core.security.SecurityExtension#getRealmSettings()}
 */
public class RealmSettings {

    public static final String PREFIX = setting("authc.realms.");

    static final Setting<String> TYPE_SETTING = Setting.simpleString("type", Setting.Property.NodeScope);
    static final Setting<Boolean> ENABLED_SETTING = Setting.boolSetting("enabled", true, Setting.Property.NodeScope);
    static final Setting<Integer> ORDER_SETTING = Setting.intSetting("order", Integer.MAX_VALUE, Setting.Property.NodeScope);

    /**
     * Add the {@link Setting} configuration for <em>all</em> realms to the provided list.
     */
    public static void addSettings(List<Setting<?>> settingsList, List<SecurityExtension> extensions) {
        settingsList.add(getGroupSetting(extensions));
    }

    public static Collection<String> getSettingsFilter(List<SecurityExtension> extensions) {
        return getSettingsByRealm(extensions).values().stream()
                .flatMap(Collection::stream)
                .filter(Setting::isFiltered)
                .map(setting -> PREFIX + "*." + setting.getKey())
                .collect(Collectors.toSet());
    }

    /**
     * Extract the child {@link Settings} for the {@link #PREFIX realms prefix}.
     * The top level names in the returned <code>Settings</code> will be the names of the configured realms.
     */
    public static Settings get(Settings settings) {
        return settings.getByPrefix(RealmSettings.PREFIX);
    }

    /**
     * Extracts the realm settings from a global settings object.
     * Returns a Map of realm-name to realm-settings.
     */
    public static Map<String, Settings> getRealmSettings(Settings globalSettings) {
        Settings realmsSettings = RealmSettings.get(globalSettings);
        return realmsSettings.names().stream()
                .collect(Collectors.toMap(Function.identity(), realmsSettings::getAsSettings));
    }

    /**
     * Convert the child {@link Setting} for the provided realm into a fully scoped key for use in an error message.
     * @see #PREFIX
     */
    public static String getFullSettingKey(RealmConfig realm, Setting<?> setting) {
        return getFullSettingKey(realm.name(), setting);
    }

    /**
     * @see #getFullSettingKey(RealmConfig, Setting)
     */
    public static String getFullSettingKey(RealmConfig realm, String subKey) {
        return getFullSettingKey(realm.name(), subKey);
    }

    private static String getFullSettingKey(String name, Setting<?> setting) {
        return getFullSettingKey(name, setting.getKey());
    }

    private static String getFullSettingKey(String name, String subKey) {
        return PREFIX + name + "." + subKey;
    }

    private static Setting<Settings> getGroupSetting(List<SecurityExtension> extensions) {
        return Setting.groupSetting(PREFIX, getSettingsValidator(extensions), Setting.Property.NodeScope);
    }

    private static Consumer<Settings> getSettingsValidator(List<SecurityExtension> extensions) {
        final Map<String, Set<Setting<?>>> childSettings = getSettingsByRealm(extensions);
        childSettings.forEach(RealmSettings::verify);
        return validator(childSettings);
    }

    /**
     * @return A map from <em>realm-type</em> to a collection of <code>Setting</code> objects.
     * @see InternalRealmsSettings#getSettings()
     */
    private static Map<String, Set<Setting<?>>> getSettingsByRealm(List<SecurityExtension> extensions) {
        final Map<String, Set<Setting<?>>> settingsByRealm = new HashMap<>(InternalRealmsSettings.getSettings());
        if (extensions != null) {
            extensions.forEach(ext -> {
                final Map<String, Set<Setting<?>>> extSettings = ext.getRealmSettings();
                extSettings.keySet().stream().filter(settingsByRealm::containsKey).forEach(type -> {
                    throw new IllegalArgumentException("duplicate realm type " + type);
                });
                settingsByRealm.putAll(extSettings);
            });
        }
        return settingsByRealm;
    }

    private static void verify(String type, Set<Setting<?>> settings) {
        Set<String> keys = new HashSet<>();
        settings.forEach(setting -> {
            final String key = setting.getKey();
            if (keys.contains(key)) {
                throw new IllegalArgumentException("duplicate setting for key " + key + " in realm type " + type);
            }
            keys.add(key);
            if (setting.getProperties().contains(Setting.Property.NodeScope) == false) {
                throw new IllegalArgumentException("setting " + key + " in realm type " + type + " does not have NodeScope");
            }
        });
    }

    private static Consumer<Settings> validator(Map<String, Set<Setting<?>>> validSettings) {
        return (settings) -> settings.names().forEach(n -> validateRealm(n, settings.getAsSettings(n), validSettings));
    }

    private static void validateRealm(String name, Settings settings, Map<String, Set<Setting<?>>> validSettings) {
        final String type = getRealmType(settings);
        if (isNullOrEmpty(type)) {
            throw new IllegalArgumentException("missing realm type [" + getFullSettingKey(name, TYPE_SETTING) + "] for realm");
        }
        validateRealm(name, type, settings, validSettings.get(type));
    }

    public static String getRealmType(Settings settings) {
        return TYPE_SETTING.get(settings);
    }

    private static void validateRealm(String name, String type, Settings settings, Set<Setting<?>> validSettings) {
        if (validSettings == null) {
            // For backwards compatibility, we assume that is we don't know the valid settings for a realm.type then everything
            // is valid. Ideally we would reject these, but XPackExtension doesn't enforce that realm-factories and realm-settings are
            // perfectly aligned
            return;
        }

        // Don't validate secure settings because they might have been cleared already
        settings = Settings.builder().put(settings, false).build();
        validSettings.removeIf(s -> s instanceof SecureSetting);

        Set<Setting<?>> settingSet = new HashSet<>(validSettings);
        settingSet.add(TYPE_SETTING);
        settingSet.add(ENABLED_SETTING);
        settingSet.add(ORDER_SETTING);
        final AbstractScopedSettings validator =
                new AbstractScopedSettings(settings, settingSet, Collections.emptySet(), Setting.Property.NodeScope) { };
        try {
            validator.validate(settings, false);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("incorrect configuration for realm [" + getFullSettingKey(name, "")
                    + "] of type " + type, e);
        }
    }

    private RealmSettings() {
    }
}
