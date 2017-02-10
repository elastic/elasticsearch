/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Bridges {@link SSLConfiguration} into the {@link Settings} framework, using {@link Setting} objects.
 */
public class SSLConfigurationSettings {

    private final String prefix;

    public final Setting<List<String>> ciphers;
    public final Setting<List<String>> supportedProtocols;
    public final Setting<Optional<String>> keystorePath;
    public final Setting<Optional<String>> keystorePassword;
    public final Setting<String> keystoreAlgorithm;
    public final Setting<Optional<String>> keystoreKeyPassword;
    public final Setting<Optional<String>> truststorePath;
    public final Setting<Optional<String>> truststorePassword;
    public final Setting<String> truststoreAlgorithm;
    public final Setting<Optional<String>> keyPath;
    public final Setting<Optional<String>> keyPassword;
    public final Setting<Optional<String>> cert;
    public final Setting<List<String>> caPaths;
    public final Setting<Optional<SSLClientAuth>> clientAuth;
    public final Setting<Optional<VerificationMode>> verificationMode;

    /**
     * @see #withoutPrefix
     * @see #withPrefix
     * @param prefix The prefix under which each setting should be defined. Must be either the empty string (<code>""</code>) or a string
     *               ending in <code>"."</code>
     */
    private SSLConfigurationSettings(String prefix) {
        assert prefix != null : "Prefix cannot be null (but can be blank)";
        this.prefix = prefix;

        ciphers = list("cipher_suites", Collections.emptyList());
        supportedProtocols = list("supported_protocols", Collections.emptyList());
        keystorePath = optionalString("keystore.path");
        keystorePassword = optionalString("keystore.password");
        keystoreKeyPassword = optionalString("keystore.key_password", keystorePassword);
        truststorePath = optionalString("truststore.path");
        truststorePassword = optionalString("truststore.password");
        keystoreAlgorithm = systemProperty("keystore.algorithm",
                "ssl.KeyManagerFactory.algorithm", KeyManagerFactory.getDefaultAlgorithm());
        truststoreAlgorithm = systemProperty("truststore.algorithm", "ssl.TrustManagerFactory.algorithm",
                TrustManagerFactory.getDefaultAlgorithm());
        keyPath = optionalString("key");
        keyPassword = optionalString("key_passphrase");
        cert = optionalString("certificate");
        caPaths = list("certificate_authorities", Collections.emptyList());
        clientAuth = optional("client_authentication", SSLClientAuth::parse);
        verificationMode = optional("verification_mode", VerificationMode::parse);
    }

    public List<Setting<?>> getAllSettings() {
        return Arrays.asList(ciphers, supportedProtocols,
                keystorePath, keystorePassword, keystoreAlgorithm, keystoreKeyPassword,
                truststorePath, truststorePassword, truststoreAlgorithm,
                keyPath, keyPassword,
                cert, caPaths, clientAuth, verificationMode);
    }

    private Setting<Optional<String>> optionalString(String keyPart) {
        return optionalString(keyPart, (s) -> null);
    }

    private Setting<Optional<String>> optionalString(String keyPart, Function<Settings, String> defaultValue) {
        return new Setting<>(prefix + keyPart, defaultValue, Optional::ofNullable,
                Setting.Property.NodeScope, Setting.Property.Filtered);
    }

    private Setting<Optional<String>> optionalString(String keyPart, Setting<Optional<String>> fallback) {
        return new Setting<>(prefix + keyPart, fallback, Optional::ofNullable,
                Setting.Property.NodeScope, Setting.Property.Filtered);
    }

    private <T> Setting<Optional<T>> optional(String keyPart, Function<String, T> parserIfNotNull) {
        Function<String,Optional<T>> parser = s -> {
            if (s == null) {
                return Optional.empty();
            } else {
                return Optional.of(parserIfNotNull.apply(s));
            }
        };
        return new Setting<>(prefix + keyPart, (String) null, parser, Setting.Property.NodeScope, Setting.Property.Filtered);
    }

    private Setting<String> systemProperty(String keyPart, String systemProperty, String defaultValue) {
        return string(keyPart, s -> System.getProperty(systemProperty, defaultValue));
    }

    private Setting<String> string(String keyPart, Function<Settings, String> defaultFunction) {
        return new Setting<>(prefix + keyPart, defaultFunction, Function.identity(),
                Setting.Property.NodeScope, Setting.Property.Filtered);
    }

    private Setting<List<String>> list(String keyPart, List<String> defaultValue) {
        return Setting.listSetting(prefix + keyPart, defaultValue, Function.identity(),
                Setting.Property.NodeScope, Setting.Property.Filtered);
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
