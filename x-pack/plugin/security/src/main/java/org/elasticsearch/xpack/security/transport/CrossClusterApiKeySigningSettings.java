/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.ssl.SslConfigurationKeys;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.net.ssl.KeyManagerFactory;

public class CrossClusterApiKeySigningSettings {
    static final String SETTINGS_PART_SIGNING = "signing";
    static final String SETTINGS_PART_DIAGNOSE_TRUST = "diagnose.trust";
    static final String KEYSTORE_ALIAS_SUFFIX = "keystore.alias";

    static final Setting.AffixSetting<String> SIGNING_KEYSTORE_ALIAS = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + KEYSTORE_ALIAS_SUFFIX,
        key -> Setting.simpleString(key, newKey -> {

        }, Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Dynamic)
    );

    static final Setting.AffixSetting<String> SIGNING_KEYSTORE_PATH = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.KEYSTORE_PATH,
        key -> Setting.simpleString(key, Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Dynamic)
    );

    static final Setting.AffixSetting<SecureString> SIGNING_KEYSTORE_SECURE_PASSWORD = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.KEYSTORE_SECURE_PASSWORD,
        key -> SecureSetting.secureString(key, null)
    );

    static final Setting.AffixSetting<String> SIGNING_KEYSTORE_ALGORITHM = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.KEYSTORE_ALGORITHM,
        key -> Setting.simpleString(
            key,
            KeyManagerFactory.getDefaultAlgorithm(),
            Setting.Property.NodeScope,
            Setting.Property.Filtered,
            Setting.Property.Dynamic
        )
    );

    static final Setting.AffixSetting<String> SIGNING_KEYSTORE_TYPE = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.KEYSTORE_TYPE,
        key -> Setting.simpleString(key, "", Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Dynamic)
    );

    static final Setting.AffixSetting<SecureString> SIGNING_KEYSTORE_SECURE_KEY_PASSWORD = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.KEYSTORE_SECURE_KEY_PASSWORD,
        key -> SecureSetting.secureString(key, null)
    );

    static final Setting.AffixSetting<String> SIGNING_KEY_PATH = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.KEY,
        key -> Setting.simpleString(key, Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Dynamic)
    );

    static final Setting.AffixSetting<SecureString> SIGNING_KEY_SECURE_PASSPHRASE = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.KEY_SECURE_PASSPHRASE,
        key -> SecureSetting.secureString(key, null)
    );

    static final Setting.AffixSetting<String> SIGNING_CERT_PATH = Setting.affixKeySetting(
        "cluster.remote.",
        SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.CERTIFICATE,
        key -> Setting.simpleString(key, Setting.Property.NodeScope, Setting.Property.Filtered, Setting.Property.Dynamic)
    );

    static final Setting<List<String>> SIGNING_CERTIFICATE_AUTHORITIES = Setting.listSetting(
        "cluster.remote." + SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.CERTIFICATE_AUTHORITIES,
        List.of(),
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Filtered,
        Setting.Property.Dynamic
    );

    static final Setting<String> SIGNING_TRUSTSTORE_PATH = Setting.simpleString(
        "cluster.remote." + SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.TRUSTSTORE_PATH,
        Setting.Property.NodeScope,
        Setting.Property.Filtered,
        Setting.Property.Dynamic
    );

    static final Setting<SecureString> SIGNING_TRUSTSTORE_SECURE_PASSWORD = SecureSetting.secureString(
        "cluster.remote." + SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.TRUSTSTORE_SECURE_PASSWORD,
        null
    );

    static final Setting<String> SIGNING_TRUSTSTORE_ALGORITHM = Setting.simpleString(
        "cluster.remote." + SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.TRUSTSTORE_ALGORITHM,
        KeyManagerFactory.getDefaultAlgorithm(),
        Setting.Property.NodeScope,
        Setting.Property.Filtered,
        Setting.Property.Dynamic
    );

    static final Setting<String> SIGNING_TRUSTSTORE_TYPE = Setting.simpleString(
        "cluster.remote." + SETTINGS_PART_SIGNING + "." + SslConfigurationKeys.TRUSTSTORE_TYPE,
        "",
        Setting.Property.NodeScope,
        Setting.Property.Filtered,
        Setting.Property.Dynamic
    );

    static final Setting<Boolean> DIAGNOSE_TRUST_EXCEPTIONS = Setting.boolSetting(
        "cluster.remote." + SETTINGS_PART_SIGNING + "." + SETTINGS_PART_DIAGNOSE_TRUST,
        true,
        Setting.Property.NodeScope,
        Setting.Property.Filtered,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> getDynamicTrustSettings() {
        return List.of(
            SIGNING_TRUSTSTORE_TYPE,
            SIGNING_TRUSTSTORE_ALGORITHM,
            SIGNING_TRUSTSTORE_PATH,
            SIGNING_CERTIFICATE_AUTHORITIES,
            DIAGNOSE_TRUST_EXCEPTIONS
        );
    }

    public static List<Setting.AffixSetting<?>> getDynamicSigningSettings() {
        return List.of(
            SIGNING_KEYSTORE_ALIAS,
            SIGNING_KEYSTORE_PATH,
            SIGNING_KEYSTORE_ALGORITHM,
            SIGNING_KEYSTORE_TYPE,
            SIGNING_KEY_PATH,
            SIGNING_CERT_PATH
        );
    }

    public static List<Setting<?>> getSecureSettings() {
        return List.of(
            SIGNING_KEYSTORE_SECURE_PASSWORD,
            SIGNING_KEYSTORE_SECURE_KEY_PASSWORD,
            SIGNING_KEY_SECURE_PASSPHRASE,
            SIGNING_TRUSTSTORE_SECURE_PASSWORD
        );
    }

    public static List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(getSecureSettings());
        settings.addAll(getDynamicSigningSettings());
        settings.addAll(getDynamicTrustSettings());
        return settings;
    }
}
