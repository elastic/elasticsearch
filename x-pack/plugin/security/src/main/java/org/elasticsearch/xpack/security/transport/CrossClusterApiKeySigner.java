/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.net.ssl.X509KeyManager;

import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.KEYSTORE_ALIAS_SUFFIX;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignerSettings.SETTINGS_PART_SIGNING;

public class CrossClusterApiKeySigner {
    private static final Map<String, String> SIGNATURE_ALGORITHM_BY_TYPE = Map.of("RSA", "SHA256withRSA", "EC", "SHA256withECDSA");

    private final Logger logger = LogManager.getLogger(getClass());
    private final Environment environment;
    private final Map<String, SigningConfig> signingConfigByClusterAlias = new ConcurrentHashMap<>();

    public CrossClusterApiKeySigner(Environment environment) {
        this.environment = environment;
        loadSigningConfigs();
    }

    Optional<SigningConfig> loadSigningConfig(String clusterAlias, Settings settings) {
        logger.trace("Loading signing config for [{}] with settings [{}]", clusterAlias, settings);
        if (settings.getByPrefix(SETTINGS_PART_SIGNING).isEmpty() == false) {
            try {
                SslKeyConfig keyConfig = CertParsingUtils.createKeyConfig(settings, SETTINGS_PART_SIGNING + ".", environment, false);
                if (keyConfig.hasKeyMaterial()) {
                    String alias = settings.get(SETTINGS_PART_SIGNING + "." + KEYSTORE_ALIAS_SUFFIX);
                    X509KeyManager keyManager = keyConfig.createKeyManager();
                    if (keyManager == null) {
                        throw new IllegalStateException("Cannot create key manager for key config [" + keyConfig + "]");
                    }

                    var keyPair = Strings.isNullOrEmpty(alias)
                        ? buildKeyPair(keyManager, keyConfig)
                        : buildKeyPair(keyManager, keyConfig, alias);

                    logger.trace("Key pair [{}] found for [{}]", keyPair, clusterAlias);
                    var signingConfig = new SigningConfig(keyPair, keyConfig.getDependentFiles());
                    signingConfigByClusterAlias.put(clusterAlias, signingConfig);
                    return Optional.of(signingConfig);
                } else {
                    logger.error(Strings.format("No signing credentials found in signing config for cluster [%s]", clusterAlias));
                }
            } catch (Exception e) {
                throw new IllegalStateException(Strings.format("Failed to load signing config for cluster [%s]", clusterAlias), e);
            }
        }
        logger.trace("No valid signing config settings found for [{}] with settings [{}]", clusterAlias, settings);
        signingConfigByClusterAlias.remove(clusterAlias);
        return Optional.empty();
    }

    public void validateSigningConfigUpdate(String clusterAlias, Settings settings) {
        if (settings.getByPrefix(SETTINGS_PART_SIGNING).isEmpty() == false) {
            var keyConfig = CertParsingUtils.createKeyConfig(settings, SETTINGS_PART_SIGNING + ".", environment, false);
            keyConfig.getDependentFiles().stream().forEach(file -> {
                if (Files.exists(file) == false) {
                    throw new IllegalArgumentException(
                        Strings.format("File [%s] configured for remote cluster [%s] does no exist", file, clusterAlias)
                    );
                }
            });
        }
    }

    public X509CertificateSignature sign(String clusterAlias, String... headers) {
        SigningConfig signingConfig = signingConfigByClusterAlias.get(clusterAlias);
        if (signingConfig == null) {
            logger.trace("No signing config found for [{}] returning empty signature", clusterAlias);
            return null;
        }
        var keyPair = signingConfig.keyPair();
        try {
            String algorithm = keyPair.signatureAlgorithm();
            Signature signature = Signature.getInstance(algorithm);
            signature.initSign(keyPair.privateKey);
            signature.update(getSignableBytes(headers));
            final byte[] sigBytes = signature.sign();
            return new X509CertificateSignature(keyPair.certificate, algorithm, new BytesArray(sigBytes));
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchSecurityException(
                Strings.format("Failed to sign cross cluster headers for cluster [%s]", clusterAlias),
                e
            );
        }
    }

    private void loadSigningConfigs() {
        this.environment.settings().getGroups("cluster.remote.", true).forEach(this::loadSigningConfig);
    }

    private X509KeyPair buildKeyPair(X509KeyManager keyManager, SslKeyConfig keyConfig) {
        final Set<String> aliases = SIGNATURE_ALGORITHM_BY_TYPE.keySet()
            .stream()
            .map(keyType -> keyManager.getServerAliases(keyType, null))
            .filter(Objects::nonNull)
            .flatMap(Arrays::stream)
            .collect(Collectors.toSet());

        logger.trace("KeyConfig [{}] has compatible entries: [{}]", keyConfig, aliases);

        return switch (aliases.size()) {
            case 0 -> throw new IllegalStateException("Cannot find a signing key in [" + keyConfig + "]");
            case 1 -> {
                final String aliasFromKeyStore = aliases.iterator().next();
                final X509Certificate[] chain = keyManager.getCertificateChain(aliasFromKeyStore);
                yield new X509KeyPair(chain[0], keyManager.getPrivateKey(aliasFromKeyStore));
            }
            default -> throw new IllegalStateException(
                "The configured signing key store has multiple signing keys ["
                    + aliases
                    + "] but no alias has been specified in signing configuration."
            );
        };
    }

    private X509KeyPair buildKeyPair(X509KeyManager keyManager, SslKeyConfig keyConfig, String alias) {
        assert alias != null;

        final String keyType = keyManager.getPrivateKey(alias).getAlgorithm();
        if (SIGNATURE_ALGORITHM_BY_TYPE.containsKey(keyType) == false) {
            throw new IllegalStateException(
                Strings.format(
                    "The key associated with alias [%s] uses unsupported key algorithm type [%s], only %s is supported",
                    alias,
                    keyType,
                    SIGNATURE_ALGORITHM_BY_TYPE.keySet()
                )
            );
        }

        final X509Certificate[] chain = keyManager.getCertificateChain(alias);
        logger.trace("KeyConfig [{}] has entry for alias: [{}] [{}]", keyConfig, alias, chain != null);
        if (chain == null) {
            throw new IllegalStateException("Key config missing certificate chain for alias [" + alias + "]");
        }

        return new X509KeyPair(chain[0], keyManager.getPrivateKey(alias));
    }

    private static byte[] getSignableBytes(final String... headers) {
        return String.join("\n", headers).getBytes(StandardCharsets.UTF_8);
    }

    private static String calculateFingerprint(X509Certificate certificate) {
        try {
            return SslUtil.calculateFingerprint(certificate, "SHA-1");
        } catch (CertificateEncodingException e) {
            return "<?>";
        }
    }

    // visible for testing
    record X509KeyPair(X509Certificate certificate, PrivateKey privateKey, String signatureAlgorithm, String fingerprint) {
        X509KeyPair(X509Certificate certificate, PrivateKey privateKey) {
            this(
                Objects.requireNonNull(certificate),
                Objects.requireNonNull(privateKey),
                Optional.ofNullable(SIGNATURE_ALGORITHM_BY_TYPE.get(privateKey.getAlgorithm()))
                    .orElseThrow(
                        () -> new IllegalArgumentException(
                            "Unsupported Key Type ["
                                + privateKey.getAlgorithm()
                                + "] in private key for ["
                                + certificate.getSubjectX500Principal()
                                + "]"
                        )
                    ),
                calculateFingerprint(certificate)
            );
        }
    }

    record SigningConfig(X509KeyPair keyPair, Collection<Path> dependentFiles) {
        public SigningConfig {
            Objects.requireNonNull(keyPair);
            Objects.requireNonNull(dependentFiles);
        }
    }

}
