/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.ssl.SslConfigurationKeys;
import org.elasticsearch.common.util.CollectionUtils;

import java.security.KeyStore;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManagerFactory;

/**
 * An encapsulation of the configuration options for X.509 Key Pair support in X-Pack security.
 * The most common use is as the private key and associated certificate for SSL/TLS support, but it can also be used for providing
 * signing or encryption keys (if they are X.509 based).
 * This class supports using a {@link java.security.KeyStore} (with configurable {@link KeyStore#getType() type}) or PEM based files.
 */
public class X509KeyPairSettings {

    static final Function<String, Setting<Optional<String>>> KEYSTORE_PATH_TEMPLATE = key -> new Setting<>(
        key,
        s -> null,
        Optional::ofNullable,
        Setting.Property.NodeScope,
        Setting.Property.Filtered
    );

    static final Function<String, Setting<SecureString>> LEGACY_KEYSTORE_PASSWORD_TEMPLATE = key -> new Setting<>(
        key,
        "",
        SecureString::new,
        Setting.Property.DeprecatedWarning,
        Setting.Property.Filtered,
        Setting.Property.NodeScope
    );
    static final Function<String, Setting<SecureString>> KEYSTORE_PASSWORD_TEMPLATE = key -> SecureSetting.secureString(
        key,
        LEGACY_KEYSTORE_PASSWORD_TEMPLATE.apply(
            key.replace(SslConfigurationKeys.KEYSTORE_SECURE_PASSWORD, SslConfigurationKeys.KEYSTORE_LEGACY_PASSWORD)
        )
    );

    static final Function<String, Setting<String>> KEY_STORE_ALGORITHM_TEMPLATE = key -> new Setting<>(
        key,
        s -> KeyManagerFactory.getDefaultAlgorithm(),
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Filtered
    );

    static final Function<String, Setting<Optional<String>>> KEY_STORE_TYPE_TEMPLATE = key -> new Setting<>(
        key,
        s -> null,
        Optional::ofNullable,
        Setting.Property.NodeScope,
        Setting.Property.Filtered
    );

    static final Function<String, Setting<SecureString>> LEGACY_KEYSTORE_KEY_PASSWORD_TEMPLATE = key -> new Setting<>(
        key,
        "",
        SecureString::new,
        Setting.Property.DeprecatedWarning,
        Setting.Property.Filtered,
        Setting.Property.NodeScope
    );
    static final Function<String, Setting<SecureString>> KEYSTORE_KEY_PASSWORD_TEMPLATE = key -> SecureSetting.secureString(
        key,
        LEGACY_KEYSTORE_KEY_PASSWORD_TEMPLATE.apply(
            key.replace(SslConfigurationKeys.KEYSTORE_SECURE_KEY_PASSWORD, SslConfigurationKeys.KEYSTORE_LEGACY_KEY_PASSWORD)
        )
    );

    static final Function<String, Setting<Optional<String>>> KEY_PATH_TEMPLATE = key -> new Setting<>(
        key,
        s -> null,
        Optional::ofNullable,
        Setting.Property.NodeScope,
        Setting.Property.Filtered
    );

    static final Function<String, Setting<Optional<String>>> CERT_TEMPLATE = key -> new Setting<>(
        key,
        s -> null,
        Optional::ofNullable,
        Setting.Property.NodeScope,
        Setting.Property.Filtered
    );

    static final Function<String, Setting<SecureString>> LEGACY_KEY_PASSWORD_TEMPLATE = key -> new Setting<>(
        key,
        "",
        SecureString::new,
        Setting.Property.DeprecatedWarning,
        Setting.Property.Filtered,
        Setting.Property.NodeScope
    );
    static final Function<String, Setting<SecureString>> KEY_PASSWORD_TEMPLATE = key -> SecureSetting.secureString(
        key,
        LEGACY_KEY_PASSWORD_TEMPLATE.apply(
            key.replace(SslConfigurationKeys.KEY_SECURE_PASSPHRASE, SslConfigurationKeys.KEY_LEGACY_PASSPHRASE)
        )
    );

    // Specify private cert/key pair via keystore
    final Setting<Optional<String>> keystorePath;
    final Setting<SecureString> keystorePassword;
    final Setting<String> keystoreAlgorithm;
    final Setting<Optional<String>> keystoreType;
    final Setting<SecureString> keystoreKeyPassword;

    // Specify private cert/key pair via key and certificate files
    final Setting<Optional<String>> keyPath;
    final Setting<SecureString> keyPassword;
    final Setting<Optional<String>> certificatePath;

    // Optional support for legacy (non secure) passwords
    // pkg private for tests
    final Setting<SecureString> legacyKeystorePassword;
    final Setting<SecureString> legacyKeystoreKeyPassword;
    final Setting<SecureString> legacyKeyPassword;

    private final List<Setting<?>> enabledSettings;
    private final List<Setting<?>> disabledSettings;

    private interface SettingFactory {
        <T> Setting<T> apply(String keyPart, Function<String, Setting<T>> template);
    }

    private X509KeyPairSettings(boolean acceptNonSecurePasswords, SettingFactory factory) {
        keystorePath = factory.apply(SslConfigurationKeys.KEYSTORE_PATH, KEYSTORE_PATH_TEMPLATE);
        keystorePassword = factory.apply(SslConfigurationKeys.KEYSTORE_SECURE_PASSWORD, KEYSTORE_PASSWORD_TEMPLATE);
        keystoreAlgorithm = factory.apply(SslConfigurationKeys.KEYSTORE_ALGORITHM, KEY_STORE_ALGORITHM_TEMPLATE);
        keystoreType = factory.apply(SslConfigurationKeys.KEYSTORE_TYPE, KEY_STORE_TYPE_TEMPLATE);
        keystoreKeyPassword = factory.apply(SslConfigurationKeys.KEYSTORE_SECURE_KEY_PASSWORD, KEYSTORE_KEY_PASSWORD_TEMPLATE);

        keyPath = factory.apply(SslConfigurationKeys.KEY, KEY_PATH_TEMPLATE);
        keyPassword = factory.apply(SslConfigurationKeys.KEY_SECURE_PASSPHRASE, KEY_PASSWORD_TEMPLATE);
        certificatePath = factory.apply(SslConfigurationKeys.CERTIFICATE, CERT_TEMPLATE);

        legacyKeystorePassword = factory.apply(SslConfigurationKeys.KEYSTORE_LEGACY_PASSWORD, LEGACY_KEYSTORE_PASSWORD_TEMPLATE);
        legacyKeystoreKeyPassword = factory.apply(SslConfigurationKeys.KEYSTORE_LEGACY_KEY_PASSWORD, LEGACY_KEYSTORE_KEY_PASSWORD_TEMPLATE);
        legacyKeyPassword = factory.apply(SslConfigurationKeys.KEY_LEGACY_PASSPHRASE, LEGACY_KEY_PASSWORD_TEMPLATE);

        final List<Setting<?>> enabled = CollectionUtils.arrayAsArrayList(
            keystorePath,
            keystorePassword,
            keystoreAlgorithm,
            keystoreType,
            keystoreKeyPassword,
            keyPath,
            keyPassword,
            certificatePath
        );

        final List<Setting<?>> legacySettings = List.of(legacyKeystorePassword, legacyKeystoreKeyPassword, legacyKeyPassword);
        if (acceptNonSecurePasswords) {
            enabled.addAll(legacySettings);
            disabledSettings = List.of();
        } else {
            disabledSettings = legacySettings;
        }

        enabledSettings = List.copyOf(enabled);
    }

    public static X509KeyPairSettings withPrefix(String prefix, boolean acceptNonSecurePasswords) {
        return new X509KeyPairSettings(acceptNonSecurePasswords, new SettingFactory() {
            @Override
            public <T> Setting<T> apply(String key, Function<String, Setting<T>> template) {
                return template.apply(prefix + key);
            }
        });
    }

    public static Collection<Setting.AffixSetting<?>> affix(String prefix, String suffixPart, boolean acceptNonSecurePasswords) {
        final X509KeyPairSettings settings = new X509KeyPairSettings(acceptNonSecurePasswords, new SettingFactory() {
            @Override
            public <T> Setting<T> apply(String keyPart, Function<String, Setting<T>> template) {
                return Setting.affixKeySetting(prefix, suffixPart + keyPart, template);
            }
        });
        return settings.getEnabledSettings().stream().map(s -> (Setting.AffixSetting<?>) s).collect(Collectors.toList());
    }

    public Collection<Setting<?>> getEnabledSettings() {
        return enabledSettings;
    }

    public Collection<Setting<?>> getDisabledSettings() {
        return disabledSettings;
    }
}
