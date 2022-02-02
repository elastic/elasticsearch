/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.security.authc.esnative.NativeRealmSettings;
import org.elasticsearch.xpack.core.security.authc.file.FileRealmSettings;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ANONYMOUS_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ANONYMOUS_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.ATTACH_REALM_TYPE;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.FALLBACK_REALM_NAME;
import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.FALLBACK_REALM_TYPE;

/**
 * Provides a number of utility methods for interacting with {@link Settings} and {@link Setting} inside a {@link Realm}.
 * Settings for realms use an {@link Setting#affixKeySetting(String, String, Function, Setting.AffixSettingDependency[]) affix} style,
 * where the <em>type</em> of the realm is part of the prefix, and name of the realm is the variable portion
 * (That is to set the order in a file realm named "file1", then full setting key would be
 * {@code xpack.security.authc.realms.file.file1.order}.
 * This class provides some convenience methods for defining and retrieving such settings.
 */
public class RealmSettings {

    public static final Setting.AffixSetting<List<String>> DOMAIN_TO_REALM_ASSOC_SETTING = Setting.affixKeySetting(
        "xpack.security.authc.domains.",
        "realms",
        key -> Setting.stringListSetting(key, Setting.Property.NodeScope)
    );

    public static final String RESERVED_REALM_AND_DOMAIN_NAME_PREFIX = "_";
    public static final String PREFIX = "xpack.security.authc.realms.";

    public static final Function<String, Setting.AffixSetting<Boolean>> ENABLED_SETTING = affixSetting(
        "enabled",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope)
    );
    public static final Function<String, Setting.AffixSetting<Integer>> ORDER_SETTING = affixSetting(
        "order",
        key -> Setting.intSetting(key, Integer.MAX_VALUE, Setting.Property.NodeScope)
    );

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
     *                       See {@link Setting#affixKeySetting(String, String, Function, Setting.AffixSettingDependency[])}
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
        return settingsByType.names().stream().flatMap(type -> {
            final Settings settingsByName = settingsByType.getAsSettings(type);
            return settingsByName.names().stream().map(name -> {
                final RealmConfig.RealmIdentifier id = new RealmConfig.RealmIdentifier(type, name);
                final Settings realmSettings = settingsByName.getAsSettings(name);
                verifyRealmSettings(id, realmSettings);
                return new Tuple<>(id, realmSettings);
            });
        }).collect(Collectors.toMap(Tuple::v1, Tuple::v2));
    }

    /**
     * Returns the domain name that the given realm is assigned to.
     * Assumes {@code #verifyRealmNameToDomainNameAssociation} successfully verified the configuration.
     */
    public static @Nullable String getDomainForRealm(Settings globalSettings, RealmConfig.RealmIdentifier realmIdentifier) {
        // TODO reserved realm settings need to be pulled into core
        if (realmIdentifier.equals(new RealmConfig.RealmIdentifier("reserved", "reserved"))
            || realmIdentifier.equals(
                new RealmConfig.RealmIdentifier(AuthenticationField.API_KEY_REALM_NAME, AuthenticationField.API_KEY_REALM_TYPE)
            )
            || realmIdentifier.equals(new RealmConfig.RealmIdentifier(FALLBACK_REALM_NAME, FALLBACK_REALM_TYPE))
            || realmIdentifier.equals(new RealmConfig.RealmIdentifier(ANONYMOUS_REALM_NAME, ANONYMOUS_REALM_TYPE))
            || realmIdentifier.equals(new RealmConfig.RealmIdentifier(ATTACH_REALM_NAME, ATTACH_REALM_TYPE))
            || realmIdentifier.equals(
                new RealmConfig.RealmIdentifier(ServiceAccountSettings.REALM_NAME, ServiceAccountSettings.REALM_TYPE)
            )) {
            return null;
        }
        // file and native realms can be referred to by their default names too
        for (String domainName : DOMAIN_TO_REALM_ASSOC_SETTING.getNamespaces(globalSettings)) {
            Setting<List<String>> realmsByDomainSetting = DOMAIN_TO_REALM_ASSOC_SETTING.getConcreteSettingForNamespace(domainName);
            if (realmsByDomainSetting.get(globalSettings).contains(realmIdentifier.getName())) {
                return domainName;
            }
        }
        return null;
    }

    /**
     * Verifies that realms are assigned to at most one domain and that domains do not refer to undefined realms.
     * Must be invoked once on node start-up (and usually not by cmd line tools).
     */
    public static void verifyRealmNameToDomainNameAssociation(
        Settings globalSettings,
        Collection<RealmConfig.RealmIdentifier> allRealmIdentifiers
    ) {
        final Map<String, Set<String>> realmToDomainsMap = new HashMap<>();
        for (String domainName : DOMAIN_TO_REALM_ASSOC_SETTING.getNamespaces(globalSettings)) {
            if (domainName.startsWith(RESERVED_REALM_AND_DOMAIN_NAME_PREFIX)) {
                throw new IllegalArgumentException(
                    "Security domain name must not start with \"" + RESERVED_REALM_AND_DOMAIN_NAME_PREFIX + "\""
                );
            }
            Setting<List<String>> realmsByDomainSetting = DOMAIN_TO_REALM_ASSOC_SETTING.getConcreteSettingForNamespace(domainName);
            for (String realmName : realmsByDomainSetting.get(globalSettings)) {
                realmToDomainsMap.computeIfAbsent(realmName, k -> new TreeSet<>()).add(domainName);
            }
        }
        final StringBuilder realmToMultipleDomainsErrorMessageBuilder = new StringBuilder(
            "Realms can be associated to at most one domain, but"
        );
        boolean realmToMultipleDomains = false;
        for (Map.Entry<String, Set<String>> realmToDomains : realmToDomainsMap.entrySet()) {
            if (realmToDomains.getValue().size() > 1) {
                if (realmToMultipleDomains) {
                    realmToMultipleDomainsErrorMessageBuilder.append(" and");
                }
                realmToMultipleDomainsErrorMessageBuilder.append(" realm [")
                    .append(realmToDomains.getKey())
                    .append("] is associated to domains ")
                    .append(realmToDomains.getValue());
                realmToMultipleDomains = true;
            }
        }
        if (realmToMultipleDomains) {
            throw new IllegalArgumentException(realmToMultipleDomainsErrorMessageBuilder.toString());
        }
        // default file and native realm names can be used in domain association
        boolean fileRealmConfigured = false;
        boolean nativeRealmConfigured = false;
        for (RealmConfig.RealmIdentifier identifier : allRealmIdentifiers) {
            realmToDomainsMap.remove(identifier.getName());
            if (identifier.getType().equals(FileRealmSettings.TYPE)) {
                fileRealmConfigured = true;
            }
            if (identifier.getType().equals(NativeRealmSettings.TYPE)) {
                nativeRealmConfigured = true;
            }
        }
        if (false == fileRealmConfigured) {
            realmToDomainsMap.remove(FileRealmSettings.DEFAULT_NAME);
        }
        if (false == nativeRealmConfigured) {
            realmToDomainsMap.remove(NativeRealmSettings.DEFAULT_NAME);
        }
        // verify that domain assignment does not refer to unknown realms
        if (false == realmToDomainsMap.isEmpty()) {
            final StringBuilder undefinedRealmsErrorMessageBuilder = new StringBuilder("Undefined realms ").append(
                realmToDomainsMap.keySet()
            ).append(" cannot be assigned to domains");
            throw new IllegalArgumentException(undefinedRealmsErrorMessageBuilder.toString());
        }
    }

    /**
     * Performs any necessary verifications on a realms settings that are not automatically applied by Settings validation infrastructure.
     */
    private static void verifyRealmSettings(RealmConfig.RealmIdentifier identifier, Settings realmSettings) {
        final Settings nonSecureSettings = Settings.builder().put(realmSettings, false).build();
        if (nonSecureSettings.isEmpty()) {
            final String prefix = realmSettingPrefix(identifier);
            throw new SettingsException(
                "found settings for the realm [{}] (with type [{}]) in the secure settings (elasticsearch.keystore),"
                    + " but this realm does not have any settings in elasticsearch.yml."
                    + " Please remove these settings from the keystore, or update their names to match one of the realms that are"
                    + " defined in elasticsearch.yml - [{}]",
                identifier.getName(),
                identifier.getType(),
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

    private RealmSettings() {}
}
