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
import org.elasticsearch.common.ssl.DiagnosticTrustManager;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslUtil;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.ssl.SslSettingsLoader;

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
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.KEYSTORE_ALIAS_SUFFIX;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SETTINGS_PART_DIAGNOSE_TRUST;
import static org.elasticsearch.xpack.security.transport.CrossClusterApiKeySigningSettings.SETTINGS_PART_SIGNING;

public class CrossClusterApiKeySignatureManager {
    private final Logger logger = LogManager.getLogger(getClass());
    private final Environment environment;
    private final AtomicReference<X509ExtendedTrustManager> trustManager = new AtomicReference<>();
    private final Map<String, X509KeyPair> keyPairByClusterAlias = new ConcurrentHashMap<>();
    private final Map<String, SslConfiguration> sslSigningConfigByClusterAlias = new ConcurrentHashMap<>();
    private final AtomicReference<SslConfiguration> sslTrustConfig = new AtomicReference<>();

    private static final Map<String, String> SIGNATURE_ALGORITHM_BY_TYPE = Map.of("RSA", "SHA256withRSA", "EC", "SHA256withECDSA");

    @SuppressWarnings("this-escape")
    public CrossClusterApiKeySignatureManager(Environment environment) {
        this.environment = environment;
        loadSigningConfigs();
        loadTrustConfig();
    }

    public void reload(Settings settings) {
        logger.trace("Loading trust config with settings [{}]", settings);
        try {
            var sslConfig = loadSslConfig(environment, settings);
            var trustConfig = sslConfig.trustConfig();
            // Only load a trust manager if trust is explicitly configured or system default, to avoid using key store as trust store
            if (trustConfig.hasExplicitConfig() || trustConfig.isSystemDefault()) {
                X509ExtendedTrustManager newTrustManager = settings.getAsBoolean(
                    SETTINGS_PART_SIGNING + "." + SETTINGS_PART_DIAGNOSE_TRUST,
                    true
                ) ? wrapInDiagnosticTrustManager(trustConfig.createTrustManager()) : trustConfig.createTrustManager();

                if (newTrustManager.getAcceptedIssuers().length == 0) {
                    logger.warn("Cross cluster API Key trust configuration [{}] has no accepted certificate issuers", trustConfig);
                    trustManager.set(null);
                } else {
                    sslTrustConfig.set(sslConfig);
                    trustManager.set(newTrustManager);
                }

            } else {
                trustManager.set(null);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load trust config", e);
        }
    }

    public void reload(String clusterAlias, Settings settings) {
        logger.trace("Loading signing config for [{}] with settings [{}]", clusterAlias, settings);
        if (settings.getByPrefix(SETTINGS_PART_SIGNING).isEmpty() == false) {
            try {
                var sslConfig = loadSslConfig(environment, settings);
                sslSigningConfigByClusterAlias.put(clusterAlias, sslConfig);

                var keyConfig = sslConfig.keyConfig();
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
                    keyPairByClusterAlias.put(clusterAlias, keyPair);
                } else {
                    keyPairByClusterAlias.remove(clusterAlias);
                }
            } catch (Exception e) {
                throw new IllegalStateException(Strings.format("Failed to load signing config for cluster [%s]", clusterAlias), e);
            }
        } else {
            logger.trace("No valid signing config settings found for [{}] with settings [{}]", clusterAlias, settings);
            keyPairByClusterAlias.remove(clusterAlias);
        }
    }

    public Collection<Path> getDependentTrustFiles() {
        var sslConfig = sslTrustConfig.get();
        return sslConfig == null ? Collections.emptyList() : sslConfig.getDependentFiles();
    }

    public Collection<Path> getDependentSigningFiles(String clusterAlias) {
        var sslConfig = sslSigningConfigByClusterAlias.get(clusterAlias);
        return sslConfig == null ? Collections.emptyList() : sslConfig.getDependentFiles();
    }

    public void validate(Settings settings) {
        if (settings.getByPrefix(SETTINGS_PART_SIGNING).isEmpty() == false) {
            var sslConfig = loadSslConfig(environment, settings);
            if (sslConfig != null) {
                sslConfig.getDependentFiles().forEach(path -> {
                    if (Files.exists(path) == false) {
                        throw new IllegalArgumentException(Strings.format("Configured file [%s] not found", path));
                    }
                });
            }
        }
    }

    // Visible for testing
    X509TrustManager getTrustManager() {
        return trustManager.get();
    }

    public Verifier verifier() {
        return new Verifier();
    }

    public Signer signerForClusterAlias(String clusterAlias) {
        return keyPairByClusterAlias.containsKey(clusterAlias) ? new Signer(clusterAlias) : null;
    }

    public class Verifier {
        private Verifier() {}

        public boolean verify(X509CertificateSignature signature, String... headers) throws GeneralSecurityException {
            assert signature.certificates().length > 0 : "Signature not valid without trusted certificate chain";

            var authTrustManager = trustManager.get();
            if (authTrustManager == null) {
                logger.warn("Cannot verify signed cross-cluster headers because [cluster.remote.signing] has not trust configuration");
                throw new IllegalStateException(
                    "Cannot verify signed cross-cluster headers because [cluster.remote.signing] has not trust configuration"
                );
            }

            // Make sure the provided certificate chain is trusted
            var leaf = signature.leafCertificate();
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "checking signing chain (len={}) [{}] with leaf subject [{}] using algorithm [{}]",
                    signature.certificates().length,
                    Arrays.stream(signature.certificates())
                        .map(CrossClusterApiKeySignatureManager::calculateFingerprint)
                        .collect(Collectors.joining(",")),
                    leaf.getSubjectX500Principal().getName(X500Principal.RFC2253),
                    leaf.getPublicKey().getAlgorithm()
                );
            }

            authTrustManager.checkClientTrusted(signature.certificates(), leaf.getPublicKey().getAlgorithm());
            signature.leafCertificate().checkValidity();

            final Signature signer = Signature.getInstance(signature.algorithm());
            signer.initVerify(leaf);
            signer.update(getSignableBytes(headers));
            return signer.verify(signature.signature().array());
        }
    }

    public class Signer {
        private final String clusterAlias;

        private Signer(String clusterAlias) {
            this.clusterAlias = clusterAlias;
        }

        public X509CertificateSignature sign(String... headers) {
            var keyPair = keyPairByClusterAlias.get(clusterAlias);
            if (keyPair == null) {
                logger.trace("No signing config found for [{}] returning null signature", clusterAlias);
                return null;
            }
            try {
                String algorithm = keyPair.signatureAlgorithm();
                Signature signature = Signature.getInstance(algorithm);
                signature.initSign(keyPair.privateKey);
                signature.update(getSignableBytes(headers));
                final byte[] sigBytes = signature.sign();
                return new X509CertificateSignature(keyPair.certificates, algorithm, new BytesArray(sigBytes));
            } catch (GeneralSecurityException e) {
                throw new ElasticsearchSecurityException(
                    Strings.format("Failed to sign cross cluster headers for cluster [%s]", clusterAlias),
                    e
                );
            }
        }
    }

    public record X509KeyPair(X509Certificate[] certificates, PrivateKey privateKey, String signatureAlgorithm, String fingerprint) {
        X509KeyPair(X509Certificate[] certificates, PrivateKey privateKey) {
            this(
                Objects.requireNonNull(certificates),
                Objects.requireNonNull(privateKey),
                Optional.ofNullable(SIGNATURE_ALGORITHM_BY_TYPE.get(privateKey.getAlgorithm()))
                    .orElseThrow(
                        () -> new IllegalArgumentException(
                            "Unsupported Key Type ["
                                + privateKey.getAlgorithm()
                                + "] in private key for ["
                                + certificates[0].getSubjectX500Principal()
                                + "]"
                        )
                    ),
                calculateFingerprint(certificates[0])
            );
        }
    }

    private X509ExtendedTrustManager wrapInDiagnosticTrustManager(X509ExtendedTrustManager trustManager) {
        if (trustManager instanceof DiagnosticTrustManager == false) {
            org.apache.logging.log4j.Logger diagnosticLogger = org.apache.logging.log4j.LogManager.getLogger(DiagnosticTrustManager.class);
            return new DiagnosticTrustManager(trustManager, () -> "cluster.remote.signing", diagnosticLogger::warn);
        }
        return trustManager;
    }

    private static String calculateFingerprint(X509Certificate certificate) {
        try {
            return SslUtil.calculateFingerprint(certificate, "SHA-1");
        } catch (CertificateEncodingException e) {
            return "<?>";
        }
    }

    private static SslConfiguration loadSslConfig(Environment environment, Settings settings) {
        return SslSettingsLoader.load(settings, SETTINGS_PART_SIGNING + ".", environment);
    }

    private static byte[] getSignableBytes(final String... headers) {
        return String.join("\n", headers).getBytes(StandardCharsets.UTF_8);
    }

    private void loadSigningConfigs() {
        this.environment.settings().getGroups("cluster.remote.", true).forEach(this::reload);
    }

    private void loadTrustConfig() {
        reload(this.environment.settings().getByPrefix("cluster.remote."));
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
                yield new X509KeyPair(chain, keyManager.getPrivateKey(aliasFromKeyStore));
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

        return new X509KeyPair(chain, keyManager.getPrivateKey(alias));
    }

}
