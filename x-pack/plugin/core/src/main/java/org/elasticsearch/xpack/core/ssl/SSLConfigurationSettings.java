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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.xpack.core.XPackSettings;

import javax.net.ssl.TrustManagerFactory;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Bridges SSLConfiguration into the {@link Settings} framework, using {@link Setting} objects.
 */
public class SSLConfigurationSettings {

    final X509KeyPairSettings x509KeyPair;

    public final Setting<List<String>> ciphers;
    public final Setting<List<String>> supportedProtocols;

    public final Setting<Optional<String>> truststorePath;
    public final Setting<SecureString> truststorePassword;
    public final Setting<String> truststoreAlgorithm;
    public final Setting<Optional<String>> truststoreType;
    public final Setting<Optional<String>> trustRestrictionsPath;
    public final Setting<List<String>> caPaths;
    public final Setting<Optional<SSLClientAuth>> clientAuth;
    public final Setting<Optional<VerificationMode>> verificationMode;

    // public for PKI realm
    public final Setting<SecureString> legacyTruststorePassword;

    private final List<Setting<?>> allSettings;

    /**
     * We explicitly default to "jks" here (rather than {@link KeyStore#getDefaultType()}) for backwards compatibility.
     * Older versions of X-Pack only supported JKS and never looked at the JVM's configured default.
     */
    private static final String DEFAULT_KEYSTORE_TYPE = "jks";
    private static final String PKCS12_KEYSTORE_TYPE = "PKCS12";

    private static final Function<String, Setting<List<String>>> CIPHERS_SETTING_TEMPLATE = key -> Setting.listSetting(key, Collections
            .emptyList(), Function.identity(), propertiesFromKey(key));
    public static final Setting<List<String>> CIPHERS_SETTING_PROFILES =  Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.cipher_suites", CIPHERS_SETTING_TEMPLATE);

    static final Function<String, Setting<List<String>>> SUPPORTED_PROTOCOLS_TEMPLATE = key -> Setting.listSetting(key,
            Collections.emptyList(), Function.identity(), propertiesFromKey(key));
    public static final Setting<List<String>> SUPPORTED_PROTOCOLS_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.supported_protocols", SUPPORTED_PROTOCOLS_TEMPLATE) ;

    public static final Setting<Optional<String>> KEYSTORE_PATH_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.keystore.path", X509KeyPairSettings.KEYSTORE_PATH_TEMPLATE);

    public static final Setting<SecureString> LEGACY_KEYSTORE_PASSWORD_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.keystore.password", X509KeyPairSettings.LEGACY_KEYSTORE_PASSWORD_TEMPLATE);

    public static final Setting<SecureString> KEYSTORE_PASSWORD_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.keystore.secure_password", X509KeyPairSettings.KEYSTORE_PASSWORD_TEMPLATE);

    public static final Setting<SecureString> LEGACY_KEYSTORE_KEY_PASSWORD_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.keystore.key_password", X509KeyPairSettings.LEGACY_KEYSTORE_KEY_PASSWORD_TEMPLATE);

    public static final Setting<SecureString> KEYSTORE_KEY_PASSWORD_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.keystore.secure_key_password", X509KeyPairSettings.KEYSTORE_KEY_PASSWORD_TEMPLATE);

    private static final Function<String, Setting<Optional<String>>> TRUST_STORE_PATH_TEMPLATE = key -> new Setting<>(key, s -> null,
            Optional::ofNullable, propertiesFromKey(key));
    public static final Setting<Optional<String>> TRUST_STORE_PATH_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.truststore.path", TRUST_STORE_PATH_TEMPLATE);

    public static final Setting<Optional<String>> KEY_PATH_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.key", X509KeyPairSettings.KEY_PATH_TEMPLATE);

    private static final Function<String, Setting<SecureString>> LEGACY_TRUSTSTORE_PASSWORD_TEMPLATE =  key ->
            new Setting<>(key, "", SecureString::new, Property.Deprecated, Property.Filtered, Property.NodeScope);
    public static final Setting<SecureString> LEGACY_TRUSTSTORE_PASSWORD_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.truststore.password", LEGACY_TRUSTSTORE_PASSWORD_TEMPLATE);

    private static final Function<String, Setting<SecureString>> TRUSTSTORE_PASSWORD_TEMPLATE  =  key ->
            SecureSetting.secureString(key, LEGACY_TRUSTSTORE_PASSWORD_TEMPLATE.apply(key.replace("truststore.secure_password",
                    "truststore.password")),
                key.startsWith(XPackSettings.GLOBAL_SSL_PREFIX) ? new Property[] { Property.Deprecated } : new Property[0]);
    public static final Setting<SecureString> TRUSTSTORE_PASSWORD_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.truststore.secure_password", TRUSTSTORE_PASSWORD_TEMPLATE);

    public static final Setting<String> KEY_STORE_ALGORITHM_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.keystore.algorithm", X509KeyPairSettings.KEY_STORE_ALGORITHM_TEMPLATE);

    private static final Function<String, Setting<String>> TRUST_STORE_ALGORITHM_TEMPLATE  =  key ->
            new Setting<>(key, s -> TrustManagerFactory.getDefaultAlgorithm(),
                    Function.identity(), propertiesFromKey(key));
    public static final Setting<String> TRUST_STORE_ALGORITHM_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.truststore.algorithm", TRUST_STORE_ALGORITHM_TEMPLATE);

    public static final Setting<Optional<String>> KEY_STORE_TYPE_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.keystore.type", X509KeyPairSettings.KEY_STORE_TYPE_TEMPLATE);

    private static final Function<String, Setting<Optional<String>>> TRUST_STORE_TYPE_TEMPLATE =
            X509KeyPairSettings.KEY_STORE_TYPE_TEMPLATE;
    public static final Setting<Optional<String>> TRUST_STORE_TYPE_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.truststore.type", TRUST_STORE_TYPE_TEMPLATE);

    private static final Function<String, Setting<Optional<String>>> TRUST_RESTRICTIONS_TEMPLATE = key -> new Setting<>(key, s -> null,
            Optional::ofNullable, propertiesFromKey(key));
    public static final Setting<Optional<String>> TRUST_RESTRICTIONS_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.trust_restrictions", TRUST_RESTRICTIONS_TEMPLATE);

    public static final Setting<SecureString> LEGACY_KEY_PASSWORD_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.key_passphrase", X509KeyPairSettings.LEGACY_KEY_PASSWORD_TEMPLATE);

    public static final Setting<SecureString> KEY_PASSWORD_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.secure_key_passphrase", X509KeyPairSettings.KEY_PASSWORD_TEMPLATE);

    public static final Setting<Optional<String>> CERT_PROFILES = Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.certificate", X509KeyPairSettings.CERT_TEMPLATE);

    private static final Function<String, Setting<List<String>>> CAPATH_SETTING_TEMPLATE = key -> Setting.listSetting(key, Collections
            .emptyList(), Function.identity(), propertiesFromKey(key));
    public static final Setting<List<String>> CAPATH_SETTING_PROFILES =  Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.certificate_authorities", CAPATH_SETTING_TEMPLATE);

    private static final Function<String, Setting<Optional<SSLClientAuth>>> CLIENT_AUTH_SETTING_TEMPLATE =
            key -> new Setting<>(key, (String) null, s -> s == null ? Optional.empty() : Optional.of(SSLClientAuth.parse(s)),
                propertiesFromKey(key));
    public static final Setting<Optional<SSLClientAuth>> CLIENT_AUTH_SETTING_PROFILES =  Setting.affixKeySetting("transport.profiles.",
            "xpack.security.ssl.client_authentication", CLIENT_AUTH_SETTING_TEMPLATE);

    private static final Function<String, Setting<Optional<VerificationMode>>> VERIFICATION_MODE_SETTING_TEMPLATE =
            key -> new Setting<>(key, (String) null, s -> s == null ? Optional.empty() : Optional.of(VerificationMode.parse(s)),
                propertiesFromKey(key));
    public static final Setting<Optional<VerificationMode>> VERIFICATION_MODE_SETTING_PROFILES = Setting.affixKeySetting(
            "transport.profiles.", "xpack.security.ssl.verification_mode", VERIFICATION_MODE_SETTING_TEMPLATE);

    /**
     * @see #withoutPrefix
     * @see #withPrefix
     * @param prefix The prefix under which each setting should be defined. Must be either the empty string (<code>""</code>) or a string
     *               ending in <code>"."</code>
     */
    private SSLConfigurationSettings(String prefix) {
        assert prefix != null : "Prefix cannot be null (but can be blank)";

        x509KeyPair = new X509KeyPairSettings(prefix, true);
        ciphers =  CIPHERS_SETTING_TEMPLATE.apply(prefix + "cipher_suites");
        supportedProtocols = SUPPORTED_PROTOCOLS_TEMPLATE.apply(prefix + "supported_protocols");
        truststorePath = TRUST_STORE_PATH_TEMPLATE.apply(prefix + "truststore.path");
        legacyTruststorePassword = LEGACY_TRUSTSTORE_PASSWORD_TEMPLATE.apply(prefix + "truststore.password");
        truststorePassword = TRUSTSTORE_PASSWORD_TEMPLATE.apply(prefix + "truststore.secure_password");
        truststoreAlgorithm = TRUST_STORE_ALGORITHM_TEMPLATE.apply(prefix + "truststore.algorithm");
        truststoreType = TRUST_STORE_TYPE_TEMPLATE.apply(prefix + "truststore.type");
        trustRestrictionsPath = TRUST_RESTRICTIONS_TEMPLATE.apply(prefix + "trust_restrictions.path");
        caPaths = CAPATH_SETTING_TEMPLATE.apply(prefix + "certificate_authorities");
        clientAuth = CLIENT_AUTH_SETTING_TEMPLATE.apply(prefix + "client_authentication");
        verificationMode = VERIFICATION_MODE_SETTING_TEMPLATE.apply(prefix + "verification_mode");

        final List<Setting<? extends Object>> settings = CollectionUtils.arrayAsArrayList(ciphers, supportedProtocols,
                truststorePath, truststorePassword, truststoreAlgorithm, truststoreType, trustRestrictionsPath,
                caPaths, clientAuth, verificationMode, legacyTruststorePassword);
        settings.addAll(x509KeyPair.getAllSettings());
        this.allSettings = Collections.unmodifiableList(settings);
    }

    public static String getKeyStoreType(Setting<Optional<String>> setting, Settings settings, String path) {
        return setting.get(settings).orElseGet(() -> inferKeyStoreType(path));
    }

    private static String inferKeyStoreType(String path) {
        String name = path == null ? "" : path.toLowerCase(Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx") || name.endsWith(".pkcs12")) {
            return PKCS12_KEYSTORE_TYPE;
        } else {
            return DEFAULT_KEYSTORE_TYPE;
        }
    }

    static Property[] propertiesFromKey(String key) {
        if (key.startsWith(XPackSettings.GLOBAL_SSL_PREFIX)) {
            return new Property[] { Property.NodeScope, Property.Filtered, Property.Deprecated };
        } else {
            return new Property[] { Property.NodeScope, Property.Filtered };
        }
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


    public static Collection<Setting<?>> getProfileSettings() {
        return Arrays.asList(CIPHERS_SETTING_PROFILES, SUPPORTED_PROTOCOLS_PROFILES, KEYSTORE_PATH_PROFILES,
                LEGACY_KEYSTORE_PASSWORD_PROFILES, KEYSTORE_PASSWORD_PROFILES, LEGACY_KEYSTORE_KEY_PASSWORD_PROFILES,
                KEYSTORE_KEY_PASSWORD_PROFILES, TRUST_STORE_PATH_PROFILES, LEGACY_TRUSTSTORE_PASSWORD_PROFILES,
                TRUSTSTORE_PASSWORD_PROFILES, KEY_STORE_ALGORITHM_PROFILES, TRUST_STORE_ALGORITHM_PROFILES,
                KEY_STORE_TYPE_PROFILES, TRUST_STORE_TYPE_PROFILES, TRUST_RESTRICTIONS_PROFILES,
                KEY_PATH_PROFILES, LEGACY_KEY_PASSWORD_PROFILES, KEY_PASSWORD_PROFILES,CERT_PROFILES,CAPATH_SETTING_PROFILES,
                CLIENT_AUTH_SETTING_PROFILES, VERIFICATION_MODE_SETTING_PROFILES);
    }

    public List<Setting<SecureString>> getSecureSettingsInUse(Settings settings) {
        return Stream.of(this.truststorePassword, this.x509KeyPair.keystorePassword,
            this.x509KeyPair.keystoreKeyPassword, this.x509KeyPair.keyPassword)
            .filter(s -> s.exists(settings))
            .collect(Collectors.toList());
    }
}
