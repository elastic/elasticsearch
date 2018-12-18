/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xpack.core.XPackSettings;

import javax.net.ssl.KeyManagerFactory;

import java.security.KeyStore;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings.propertiesFromKey;

/**
 * An encapsulation of the configuration options for X.509 Key Pair support in X-Pack security.
 * The most common use is as the private key and associated certificate for SSL/TLS support, but it can also be used for providing
 * signing or encryption keys (if they are X.509 based).
 * This class supports using a {@link java.security.KeyStore} (with configurable {@link KeyStore#getType() type}) or PEM based files.
 */
public class X509KeyPairSettings {

    static final Function<String, Setting<Optional<String>>> KEYSTORE_PATH_TEMPLATE = key -> new Setting<>(key, s -> null,
            Optional::ofNullable, propertiesFromKey(key));

    static final Function<String, Setting<SecureString>> LEGACY_KEYSTORE_PASSWORD_TEMPLATE = key -> new Setting<>(key, "",
            SecureString::new, Setting.Property.Deprecated, Setting.Property.Filtered, Setting.Property.NodeScope);
    static final Function<String, Setting<SecureString>> KEYSTORE_PASSWORD_TEMPLATE = key -> SecureSetting.secureString(key,
        LEGACY_KEYSTORE_PASSWORD_TEMPLATE.apply(key.replace("keystore.secure_password", "keystore.password")),
        key.startsWith(XPackSettings.GLOBAL_SSL_PREFIX) ? new Property[] { Property.Deprecated } : new Property[0]);

    static final Function<String, Setting<String>> KEY_STORE_ALGORITHM_TEMPLATE = key ->
            new Setting<>(key, s -> KeyManagerFactory.getDefaultAlgorithm(),
                    Function.identity(), propertiesFromKey(key));

    static final Function<String, Setting<Optional<String>>> KEY_STORE_TYPE_TEMPLATE = key ->
            new Setting<>(key, s -> null, Optional::ofNullable, propertiesFromKey(key));

    static final Function<String, Setting<SecureString>> LEGACY_KEYSTORE_KEY_PASSWORD_TEMPLATE = key -> new Setting<>(key, "",
            SecureString::new, Setting.Property.Deprecated, Setting.Property.Filtered, Setting.Property.NodeScope);
    static final Function<String, Setting<SecureString>> KEYSTORE_KEY_PASSWORD_TEMPLATE = key ->
            SecureSetting.secureString(key, LEGACY_KEYSTORE_KEY_PASSWORD_TEMPLATE.apply(key.replace("keystore.secure_key_password",
                    "keystore.key_password")),
                key.startsWith(XPackSettings.GLOBAL_SSL_PREFIX) ? new Property[] { Property.Deprecated } : new Property[0]);

    static final Function<String, Setting<Optional<String>>> KEY_PATH_TEMPLATE = key -> new Setting<>(key, s -> null,
            Optional::ofNullable, propertiesFromKey(key));

    static final Function<String, Setting<Optional<String>>> CERT_TEMPLATE = key -> new Setting<>(key, s -> null,
            Optional::ofNullable, propertiesFromKey(key));

    static final Function<String, Setting<SecureString>> LEGACY_KEY_PASSWORD_TEMPLATE = key -> new Setting<>(key, "",
            SecureString::new, Setting.Property.Deprecated, Setting.Property.Filtered, Setting.Property.NodeScope);
    static final Function<String, Setting<SecureString>> KEY_PASSWORD_TEMPLATE = key ->
            SecureSetting.secureString(key, LEGACY_KEY_PASSWORD_TEMPLATE.apply(key.replace("secure_key_passphrase", "key_passphrase")),
                key.startsWith(XPackSettings.GLOBAL_SSL_PREFIX) ? new Property[] { Property.Deprecated } : new Property[0]);


    private final String prefix;

    // Specify private cert/key pair via keystore
    public final Setting<Optional<String>> keystorePath;
    public final Setting<SecureString> keystorePassword;
    public final Setting<String> keystoreAlgorithm;
    public final Setting<Optional<String>> keystoreType;
    public final Setting<SecureString> keystoreKeyPassword;

    // Specify private cert/key pair via key and certificate files
    public final Setting<Optional<String>> keyPath;
    public final Setting<SecureString> keyPassword;
    public final Setting<Optional<String>> certificatePath;

    // Optional support for legacy (non secure) passwords
    public final Setting<SecureString> legacyKeystorePassword;
    public final Setting<SecureString> legacyKeystoreKeyPassword;
    public final Setting<SecureString> legacyKeyPassword;

    private final List<Setting<?>> allSettings;

    public X509KeyPairSettings(String prefix, boolean acceptNonSecurePasswords) {
        keystorePath = KEYSTORE_PATH_TEMPLATE.apply(prefix + "keystore.path");
        keystorePassword = KEYSTORE_PASSWORD_TEMPLATE.apply(prefix + "keystore.secure_password");
        keystoreAlgorithm = KEY_STORE_ALGORITHM_TEMPLATE.apply(prefix + "keystore.algorithm");
        keystoreType = KEY_STORE_TYPE_TEMPLATE.apply(prefix + "keystore.type");
        keystoreKeyPassword = KEYSTORE_KEY_PASSWORD_TEMPLATE.apply(prefix + "keystore.secure_key_password");

        keyPath = KEY_PATH_TEMPLATE.apply(prefix + "key");
        keyPassword = KEY_PASSWORD_TEMPLATE.apply(prefix + "secure_key_passphrase");
        certificatePath = CERT_TEMPLATE.apply(prefix + "certificate");

        legacyKeystorePassword = LEGACY_KEYSTORE_PASSWORD_TEMPLATE.apply(prefix + "keystore.password");
        legacyKeystoreKeyPassword = LEGACY_KEYSTORE_KEY_PASSWORD_TEMPLATE.apply(prefix + "keystore.key_password");
        legacyKeyPassword = LEGACY_KEY_PASSWORD_TEMPLATE.apply(prefix + "key_passphrase");
        this.prefix = prefix;

        final List<Setting<?>> settings = CollectionUtils.arrayAsArrayList(
                keystorePath, keystorePassword, keystoreAlgorithm, keystoreType, keystoreKeyPassword,
                keyPath, keyPassword, certificatePath);
        if (acceptNonSecurePasswords) {
            settings.add(legacyKeystorePassword);
            settings.add(legacyKeystoreKeyPassword);
            settings.add(legacyKeyPassword);
        }
        allSettings = Collections.unmodifiableList(settings);
    }


    public Collection<? extends Setting<?>> getAllSettings() {
        return allSettings;
    }

    public String getPrefix() {
        return prefix;
    }
}
