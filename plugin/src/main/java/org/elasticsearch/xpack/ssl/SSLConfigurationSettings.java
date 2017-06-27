/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import org.elasticsearch.common.settings.Setting.Property;

/**
 * Bridges {@link SSLConfiguration} into the {@link Settings} framework, using {@link Setting} objects.
 */
public class SSLConfigurationSettings {

    public final Setting<List<String>> ciphers;
    public final Setting<List<String>> supportedProtocols;
    public final Setting<Optional<String>> keystorePath;
    public final Setting<SecureString> keystorePassword;
    public final Setting<String> keystoreAlgorithm;
    public final Setting<SecureString> keystoreKeyPassword;
    public final Setting<Optional<String>> truststorePath;
    public final Setting<SecureString> truststorePassword;
    public final Setting<String> truststoreAlgorithm;
    public final Setting<Optional<String>> keyPath;
    public final Setting<SecureString> keyPassword;
    public final Setting<Optional<String>> cert;
    public final Setting<List<String>> caPaths;
    public final Setting<Optional<SSLClientAuth>> clientAuth;
    public final Setting<Optional<VerificationMode>> verificationMode;

    // pkg private for tests
    final Setting<SecureString> legacyKeystorePassword;
    final Setting<SecureString> legacyKeystoreKeyPassword;
    final Setting<SecureString> legacyTruststorePassword;
    final Setting<SecureString> legacyKeyPassword;

    private final List<Setting<?>> allSettings;

    /**
     * @see #withoutPrefix
     * @see #withPrefix
     * @param prefix The prefix under which each setting should be defined. Must be either the empty string (<code>""</code>) or a string
     *               ending in <code>"."</code>
     */
    private SSLConfigurationSettings(String prefix) {
        assert prefix != null : "Prefix cannot be null (but can be blank)";

        ciphers = Setting.listSetting(prefix + "cipher_suites", Collections.emptyList(), Function.identity(),
            Property.NodeScope, Property.Filtered);
        supportedProtocols = Setting.listSetting(prefix + "supported_protocols", Collections.emptyList(), Function.identity(),
            Property.NodeScope, Property.Filtered);
        keystorePath = new Setting<>(prefix + "keystore.path", s -> null, Optional::ofNullable,
            Property.NodeScope, Property.Filtered);
        legacyKeystorePassword = new Setting<>(prefix + "keystore.password", "", SecureString::new,
            Property.Deprecated, Property.Filtered, Property.NodeScope);
        keystorePassword = SecureSetting.secureString(prefix + "keystore.secure_password", legacyKeystorePassword);
        legacyKeystoreKeyPassword = new Setting<>(prefix + "keystore.key_password", "",
            SecureString::new, Property.Deprecated, Property.Filtered, Property.NodeScope);
        keystoreKeyPassword = SecureSetting.secureString(prefix + "keystore.secure_key_password", legacyKeystoreKeyPassword);
        truststorePath = new Setting<>(prefix + "truststore.path", s -> null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
        legacyTruststorePassword = new Setting<>(prefix + "truststore.password", "", SecureString::new,
            Property.Deprecated, Property.Filtered, Property.NodeScope);
        truststorePassword = SecureSetting.secureString(prefix + "truststore.secure_password", legacyTruststorePassword);
        keystoreAlgorithm = new Setting<>(prefix + "keystore.algorithm", s -> KeyManagerFactory.getDefaultAlgorithm(),
            Function.identity(), Property.NodeScope, Property.Filtered);
        truststoreAlgorithm = new Setting<>(prefix + "truststore.algorithm", s -> TrustManagerFactory.getDefaultAlgorithm(),
            Function.identity(), Property.NodeScope, Property.Filtered);
        keyPath = new Setting<>(prefix + "key", s -> null, Optional::ofNullable, Setting.Property.NodeScope, Setting.Property.Filtered);
        legacyKeyPassword = new Setting<>(prefix + "key_passphrase", "", SecureString::new,
            Property.Deprecated, Property.Filtered, Property.NodeScope);
        keyPassword = SecureSetting.secureString(prefix + "secure_key_passphrase", legacyKeyPassword);
        cert =new Setting<>(prefix + "certificate", s -> null, Optional::ofNullable, Property.NodeScope, Property.Filtered);
        caPaths = Setting.listSetting(prefix + "certificate_authorities", Collections.emptyList(), Function.identity(),
            Property.NodeScope, Property.Filtered);
        clientAuth = new Setting<>(prefix + "client_authentication", (String) null,
            s -> s == null ? Optional.empty() : Optional.of(SSLClientAuth.parse(s)), Property.NodeScope, Property.Filtered);
        verificationMode = new Setting<>(prefix + "verification_mode", (String) null,
            s -> s == null ? Optional.empty() : Optional.of(VerificationMode.parse(s)), Property.NodeScope, Property.Filtered);

        this.allSettings = Arrays.asList(ciphers, supportedProtocols, keystorePath, keystorePassword, keystoreAlgorithm,
            keystoreKeyPassword, truststorePath, truststorePassword, truststoreAlgorithm, keyPath, keyPassword, cert, caPaths,
            clientAuth, verificationMode, legacyKeystorePassword, legacyKeystoreKeyPassword, legacyKeyPassword, legacyTruststorePassword);
    }

    public List<Setting<?>> getAllSettings() {
        return allSettings;
    }

    /**
     * Construct settings that are un-prefixed. That is, they can be used to read from a {@link Settings} object where the configuration
     * keys are the root names of the <code>Settings</code>.
     */
    public static SSLConfigurationSettings withoutPrefix() {
        return new SSLConfigurationSettings("");
    }

    /**
     * Construct settings that have a prefixed. That is, they can be used to read from a {@link Settings} object where the configuration
     * keys are prefixed-children of the <code>Settings</code>.
     * @param prefix A string that must end in <code>"ssl."</code>
     */
    public static SSLConfigurationSettings withPrefix(String prefix) {
        assert prefix.endsWith("ssl.") : "The ssl config prefix (" + prefix + ") should end in 'ssl.'";
        return new SSLConfigurationSettings(prefix);
    }
}
