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
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfigurationKeys;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.net.ssl.TrustManagerFactory;

/**
 * Bridges SSLConfiguration into the {@link Settings} framework, using {@link Setting} objects.
 */
public class SSLConfigurationSettings {

    final X509KeyPairSettings x509KeyPair;

    final Setting<List<String>> ciphers;
    final Setting<List<String>> supportedProtocols;

    final Setting<Optional<String>> truststorePath;
    final Setting<SecureString> truststorePassword;
    final Setting<String> truststoreAlgorithm;
    final Setting<Optional<String>> truststoreType;
    final Setting<Optional<String>> trustRestrictionsPath;
    final Setting<List<String>> caPaths;
    final Setting<Optional<SslClientAuthenticationMode>> clientAuth;
    final Setting<Optional<SslVerificationMode>> verificationMode;

    // public for PKI realm
    private final Setting<SecureString> legacyTruststorePassword;

    private final List<Setting<?>> enabledSettings;
    private final List<Setting<?>> disabledSettings;

    private static final Function<String, Setting<List<String>>> CIPHERS_SETTING_TEMPLATE = key -> Setting.listSetting(
        key,
        List.of(),
        Function.identity(),
        Property.NodeScope,
        Property.Filtered
    );
    private static final SslSetting<List<String>> CIPHERS = SslSetting.setting(SslConfigurationKeys.CIPHERS, CIPHERS_SETTING_TEMPLATE);

    private static final SslSetting<List<String>> SUPPORTED_PROTOCOLS = SslSetting.setting(
        SslConfigurationKeys.PROTOCOLS,
        key -> Setting.listSetting(key, List.of(), Function.identity(), Property.NodeScope, Property.Filtered)
    );

    private static final SslSetting<Optional<String>> KEYSTORE_PATH = SslSetting.setting(
        SslConfigurationKeys.KEYSTORE_PATH,
        X509KeyPairSettings.KEYSTORE_PATH_TEMPLATE
    );

    private static final SslSetting<SecureString> LEGACY_KEYSTORE_PASSWORD = SslSetting.setting(
        SslConfigurationKeys.KEYSTORE_LEGACY_PASSWORD,
        X509KeyPairSettings.LEGACY_KEYSTORE_PASSWORD_TEMPLATE
    );

    private static final SslSetting<SecureString> KEYSTORE_PASSWORD = SslSetting.secureSetting(
        SslConfigurationKeys.KEYSTORE_SECURE_PASSWORD,
        X509KeyPairSettings.KEYSTORE_PASSWORD_TEMPLATE
    );

    private static final SslSetting<SecureString> LEGACY_KEYSTORE_KEY_PASSWORD = SslSetting.setting(
        SslConfigurationKeys.KEYSTORE_LEGACY_KEY_PASSWORD,
        X509KeyPairSettings.LEGACY_KEYSTORE_KEY_PASSWORD_TEMPLATE
    );

    private static final SslSetting<SecureString> KEYSTORE_KEY_PASSWORD = SslSetting.secureSetting(
        SslConfigurationKeys.KEYSTORE_SECURE_KEY_PASSWORD,
        X509KeyPairSettings.KEYSTORE_KEY_PASSWORD_TEMPLATE
    );

    public static final SslSetting<Optional<String>> TRUSTSTORE_PATH = SslSetting.setting(
        SslConfigurationKeys.TRUSTSTORE_PATH,
        key -> new Setting<>(key, s -> null, Optional::ofNullable, Property.NodeScope, Property.Filtered)
    );

    private static final SslSetting<Optional<String>> KEY_PATH = SslSetting.setting(
        SslConfigurationKeys.KEY,
        X509KeyPairSettings.KEY_PATH_TEMPLATE
    );

    public static final SslSetting<SecureString> LEGACY_TRUSTSTORE_PASSWORD = SslSetting.setting(
        SslConfigurationKeys.TRUSTSTORE_LEGACY_PASSWORD,
        key -> new Setting<>(key, "", SecureString::new, Property.Deprecated, Property.Filtered, Property.NodeScope)
    );

    public static final SslSetting<SecureString> TRUSTSTORE_PASSWORD = SslSetting.secureSetting(
        SslConfigurationKeys.TRUSTSTORE_SECURE_PASSWORD,
        key -> SecureSetting.secureString(
            key,
            LEGACY_TRUSTSTORE_PASSWORD.template().apply(key.replace("truststore.secure_password", "truststore.password"))
        )
    );

    private static final SslSetting<String> KEY_STORE_ALGORITHM = SslSetting.setting(
        SslConfigurationKeys.KEYSTORE_ALGORITHM,
        X509KeyPairSettings.KEY_STORE_ALGORITHM_TEMPLATE
    );

    public static final Function<String, Setting<String>> TRUST_STORE_ALGORITHM_TEMPLATE = key -> new Setting<>(
        key,
        s -> TrustManagerFactory.getDefaultAlgorithm(),
        Function.identity(),
        Property.NodeScope,
        Property.Filtered
    );
    public static final SslSetting<String> TRUSTSTORE_ALGORITHM = SslSetting.setting(
        SslConfigurationKeys.TRUSTSTORE_ALGORITHM,
        TRUST_STORE_ALGORITHM_TEMPLATE
    );

    private static final SslSetting<Optional<String>> KEY_STORE_TYPE = SslSetting.setting(
        SslConfigurationKeys.KEYSTORE_TYPE,
        X509KeyPairSettings.KEY_STORE_TYPE_TEMPLATE
    );

    public static final Function<String, Setting<Optional<String>>> TRUST_STORE_TYPE_TEMPLATE = X509KeyPairSettings.KEY_STORE_TYPE_TEMPLATE;
    public static final SslSetting<Optional<String>> TRUSTSTORE_TYPE = SslSetting.setting(
        SslConfigurationKeys.TRUSTSTORE_TYPE,
        TRUST_STORE_TYPE_TEMPLATE
    );

    private static final Function<String, Setting<Optional<String>>> TRUST_RESTRICTIONS_TEMPLATE = key -> new Setting<>(
        key,
        s -> null,
        Optional::ofNullable,
        Property.NodeScope,
        Property.Filtered
    );
    private static final SslSetting<Optional<String>> TRUST_RESTRICTIONS = SslSetting.setting(
        "trust_restrictions.path",
        TRUST_RESTRICTIONS_TEMPLATE
    );

    private static final SslSetting<SecureString> LEGACY_KEY_PASSWORD = SslSetting.setting(
        SslConfigurationKeys.KEY_LEGACY_PASSPHRASE,
        X509KeyPairSettings.LEGACY_KEY_PASSWORD_TEMPLATE
    );

    private static final SslSetting<SecureString> KEY_PASSWORD = SslSetting.secureSetting(
        SslConfigurationKeys.KEY_SECURE_PASSPHRASE,
        X509KeyPairSettings.KEY_PASSWORD_TEMPLATE
    );

    private static final SslSetting<Optional<String>> CERT = SslSetting.setting(
        SslConfigurationKeys.CERTIFICATE,
        X509KeyPairSettings.CERT_TEMPLATE
    );

    public static final Function<String, Setting<List<String>>> CAPATH_SETTING_TEMPLATE = key -> Setting.listSetting(
        key,
        Collections.emptyList(),
        Function.identity(),
        Property.NodeScope,
        Property.Filtered
    );
    public static final SslSetting<List<String>> CERT_AUTH_PATH = SslSetting.setting(
        SslConfigurationKeys.CERTIFICATE_AUTHORITIES,
        CAPATH_SETTING_TEMPLATE
    );
    public static final Function<String, Setting.AffixSetting<List<String>>> CAPATH_SETTING_REALM = CERT_AUTH_PATH::realm;

    private static final Function<String, Setting<Optional<SslClientAuthenticationMode>>> CLIENT_AUTH_SETTING_TEMPLATE =
        key -> new Setting<>(
            key,
            (String) null,
            s -> s == null ? Optional.empty() : Optional.of(SslClientAuthenticationMode.parse(s)),
            Property.NodeScope,
            Property.Filtered
        );
    private static final SslSetting<Optional<SslClientAuthenticationMode>> CLIENT_AUTH_SETTING = SslSetting.setting(
        SslConfigurationKeys.CLIENT_AUTH,
        CLIENT_AUTH_SETTING_TEMPLATE
    );

    private static final Function<String, Setting<Optional<SslVerificationMode>>> VERIFICATION_MODE_SETTING_TEMPLATE = key -> new Setting<>(
        key,
        (String) null,
        s -> s == null ? Optional.empty() : Optional.of(SslVerificationMode.parse(s)),
        Property.NodeScope,
        Property.Filtered
    );
    private static final SslSetting<Optional<SslVerificationMode>> VERIFICATION_MODE = SslSetting.setting(
        SslConfigurationKeys.VERIFICATION_MODE,
        VERIFICATION_MODE_SETTING_TEMPLATE
    );
    public static final Function<String, Setting.AffixSetting<Optional<SslVerificationMode>>> VERIFICATION_MODE_SETTING_REALM =
        VERIFICATION_MODE::realm;

    /**
     * @param prefix The prefix under which each setting should be defined. Must be either the empty string (<code>""</code>) or a string
     *               ending in <code>"."</code>
     * @param acceptNonSecurePasswords Whether legacy (non-secure passwords should be accepted
     * @see #withoutPrefix
     * @see #withPrefix
     */
    private SSLConfigurationSettings(String prefix, boolean acceptNonSecurePasswords) {
        assert prefix != null : "Prefix cannot be null (but can be blank)";

        x509KeyPair = X509KeyPairSettings.withPrefix(prefix, acceptNonSecurePasswords);
        ciphers = CIPHERS.withPrefix(prefix);
        supportedProtocols = SUPPORTED_PROTOCOLS.withPrefix(prefix);
        truststorePath = TRUSTSTORE_PATH.withPrefix(prefix);
        legacyTruststorePassword = LEGACY_TRUSTSTORE_PASSWORD.withPrefix(prefix);
        truststorePassword = TRUSTSTORE_PASSWORD.withPrefix(prefix);
        truststoreAlgorithm = TRUSTSTORE_ALGORITHM.withPrefix(prefix);
        truststoreType = TRUSTSTORE_TYPE.withPrefix(prefix);
        trustRestrictionsPath = TRUST_RESTRICTIONS.withPrefix(prefix);
        caPaths = CERT_AUTH_PATH.withPrefix(prefix);
        clientAuth = CLIENT_AUTH_SETTING.withPrefix(prefix);
        verificationMode = VERIFICATION_MODE.withPrefix(prefix);

        final List<Setting<? extends Object>> enabled = CollectionUtils.arrayAsArrayList(
            ciphers,
            supportedProtocols,
            truststorePath,
            truststorePassword,
            truststoreAlgorithm,
            truststoreType,
            trustRestrictionsPath,
            caPaths,
            clientAuth,
            verificationMode
        );
        final List<Setting<?>> disabled = new ArrayList<>();
        if (acceptNonSecurePasswords) {
            enabled.add(legacyTruststorePassword);
        } else {
            disabled.add(legacyTruststorePassword);
        }

        enabled.addAll(x509KeyPair.getEnabledSettings());
        disabled.addAll(x509KeyPair.getDisabledSettings());

        this.enabledSettings = Collections.unmodifiableList(enabled);
        this.disabledSettings = Collections.unmodifiableList(disabled);
    }

    public List<Setting<?>> getEnabledSettings() {
        return enabledSettings;
    }

    public List<Setting<?>> getDisabledSettings() {
        return disabledSettings;
    }

    /**
     * Construct settings that are un-prefixed. That is, they can be used to read from a {@link Settings} object where the configuration
     * keys are the root names of the <code>Settings</code>.
     * @param acceptNonSecurePasswords
     */
    public static SSLConfigurationSettings withoutPrefix(boolean acceptNonSecurePasswords) {
        return new SSLConfigurationSettings("", acceptNonSecurePasswords);
    }

    /**
     * Construct settings that have a prefixed. That is, they can be used to read from a {@link Settings} object where the configuration
     * keys are prefixed-children of the <code>Settings</code>.
     *
     * @param prefix A string that must end in <code>"ssl."</code>
     * @param acceptNonSecurePasswords
     */
    public static SSLConfigurationSettings withPrefix(String prefix, boolean acceptNonSecurePasswords) {
        assert prefix.endsWith(".") : "The ssl config prefix (" + prefix + ") should end in '.'";
        return new SSLConfigurationSettings(prefix, acceptNonSecurePasswords);
    }

    private static Collection<SslSetting<?>> settings() {
        return Arrays.asList(
            CIPHERS,
            SUPPORTED_PROTOCOLS,
            KEYSTORE_PATH,
            LEGACY_KEYSTORE_PASSWORD,
            KEYSTORE_PASSWORD,
            LEGACY_KEYSTORE_KEY_PASSWORD,
            KEYSTORE_KEY_PASSWORD,
            TRUSTSTORE_PATH,
            LEGACY_TRUSTSTORE_PASSWORD,
            TRUSTSTORE_PASSWORD,
            KEY_STORE_ALGORITHM,
            TRUSTSTORE_ALGORITHM,
            KEY_STORE_TYPE,
            TRUSTSTORE_TYPE,
            TRUST_RESTRICTIONS,
            KEY_PATH,
            LEGACY_KEY_PASSWORD,
            KEY_PASSWORD,
            CERT,
            CERT_AUTH_PATH,
            CLIENT_AUTH_SETTING,
            VERIFICATION_MODE
        );
    }

    public static Collection<Setting<?>> getProfileSettings() {
        return settings().stream().map(SslSetting::transportProfile).collect(Collectors.toUnmodifiableList());
    }

    public static Collection<Setting.AffixSetting<?>> getRealmSettings(String realmType) {
        return settings().stream().map(s -> s.realm(realmType)).collect(Collectors.toList());
    }

    public List<Setting<? extends SecureString>> getSecureSettingsInUse(Settings settings) {
        return getSecureSettings().stream().filter(s -> s.exists(settings)).collect(Collectors.toList());
    }

    List<Setting<? extends SecureString>> getSecureSettings() {
        return List.of(truststorePassword, x509KeyPair.keyPassword, x509KeyPair.keystorePassword, x509KeyPair.keystoreKeyPassword);
    }

    public static class SslSetting<T> {
        protected final String name;
        protected final Function<String, Setting<T>> template;

        public SslSetting(String name, Function<String, Setting<T>> template) {
            this.name = name;
            this.template = template;
        }

        public static SslSetting<SecureString> secureSetting(String name, Function<String, Setting<SecureString>> template) {
            return new SslSetting<>(name, template);
        }

        public static <T> SslSetting<T> setting(String name, Function<String, Setting<T>> template) {
            return new SslSetting<>(name, template);
        }

        Function<String, Setting<T>> template() {
            return template;
        }

        Setting<T> rawSetting() {
            return applyTemplate(name);
        }

        public Setting<T> withPrefix(String prefix) {
            if (prefix.length() == 0) {
                return rawSetting();
            }
            if (prefix.endsWith(".")) {
                return applyTemplate(prefix + name);
            } else {
                throw new IllegalArgumentException("The ssl config prefix (" + prefix + ") should end in '.'");
            }
        }

        Setting<T> applyTemplate(String key) {
            return template.apply(key);
        }

        public Setting.AffixSetting<T> realm(String realmType) {
            return affixSetting("xpack.security.authc.realms." + realmType + ".", "ssl.");
        }

        public Setting<T> realm(RealmConfig.RealmIdentifier realmId) {
            return realm(realmId.getType()).getConcreteSettingForNamespace(realmId.getName());
        }

        Setting.AffixSetting<T> transportProfile() {
            return affixSetting("transport.profiles.", "xpack.security.ssl.");
        }

        public Setting.AffixSetting<T> affixSetting(String groupPrefix, String keyPrefix) {
            return Setting.affixKeySetting(groupPrefix, keyPrefix + name, template);
        }

        public Setting<T> transportProfile(String name) {
            return transportProfile().getConcreteSetting(name);
        }
    }

}
