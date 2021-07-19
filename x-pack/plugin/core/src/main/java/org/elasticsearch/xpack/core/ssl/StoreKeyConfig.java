/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;

/**
 * A key configuration that is backed by a {@link KeyStore}
 */
public class StoreKeyConfig extends KeyConfig {

    private static final String KEYSTORE_FILE = "keystore";

    final String keyStorePath;
    final String keyStoreType;
    final SecureString keyStorePassword;
    final String keyStoreAlgorithm;
    final SecureString keyPassword;
    final String trustStoreAlgorithm;

    /**
     * Creates a new configuration that can be used to load key and trust material from a {@link KeyStore}
     * @param keyStorePath the path to the keystore file
     * @param keyStoreType the type of the keystore file
     * @param keyStorePassword the password for the keystore
     * @param keyPassword the password for the private key in the keystore
     * @param keyStoreAlgorithm the algorithm for the keystore
     * @param trustStoreAlgorithm the algorithm to use when loading as a truststore
     */
    StoreKeyConfig(String keyStorePath, String keyStoreType, SecureString keyStorePassword, SecureString keyPassword,
                   String keyStoreAlgorithm, String trustStoreAlgorithm) {
        if (keyStoreType.equalsIgnoreCase("pkcs11")) {
            throw new IllegalArgumentException("PKCS#11 keystores are no longer supported by Elasticsearch");
        }
        this.keyStorePath = Objects.requireNonNull(keyStorePath);
        this.keyStoreType = Objects.requireNonNull(keyStoreType, "keystore type must be specified");
        // since we support reloading the keystore, we must store the passphrase in memory for the life of the node, so we
        // clone the password and never close it during our uses below
        this.keyStorePassword = Objects.requireNonNull(keyStorePassword, "keystore password must be specified").clone();
        this.keyPassword = Objects.requireNonNull(keyPassword).clone();
        this.keyStoreAlgorithm = keyStoreAlgorithm;
        this.trustStoreAlgorithm = trustStoreAlgorithm;
    }

    @Override
    X509ExtendedKeyManager createKeyManager(@Nullable Environment environment) {
        Path ksPath = CertParsingUtils.resolvePath(keyStorePath, environment);
        try {
            KeyStore ks = getStore(ksPath, keyStoreType, keyStorePassword);
            checkKeyStore(ks);
            // TBD: filter out only http.ssl.keystore
            List<String> aliases = new ArrayList<>();
            for (String s : Collections.list(ks.aliases())) {
                if (ks.isKeyEntry(s)) {
                    aliases.add(s);
                }
            }
            if (aliases.size() > 1) {
                for (String alias : aliases) {
                    Certificate certificate = ks.getCertificate(alias);
                    if (certificate instanceof X509Certificate) {
                        if (((X509Certificate) certificate).getBasicConstraints() != -1) {
                            ks.deleteEntry(alias);
                        }
                    }
                }
            }
            return CertParsingUtils.keyManager(ks, keyPassword.getChars(), keyStoreAlgorithm);
        } catch (FileNotFoundException | NoSuchFileException e) {
            throw missingKeyConfigFile(e, KEYSTORE_FILE, ksPath);
        } catch (AccessDeniedException e) {
            throw unreadableKeyConfigFile(e, KEYSTORE_FILE, ksPath);
        } catch (AccessControlException e) {
            throw blockedKeyConfigFile(e, environment, KEYSTORE_FILE, ksPath);
        } catch (IOException | GeneralSecurityException e) {
            throw new ElasticsearchException("failed to initialize SSL KeyManager", e);
        }
    }

    @Override
    X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
        final Path ksPath = CertParsingUtils.resolvePath(keyStorePath, environment);
        try {
            KeyStore ks = getStore(ksPath, keyStoreType, keyStorePassword);
            return CertParsingUtils.trustManager(ks, trustStoreAlgorithm);
        } catch (FileNotFoundException | NoSuchFileException e) {
            throw missingTrustConfigFile(e, KEYSTORE_FILE, ksPath);
        } catch (AccessDeniedException e) {
            throw missingTrustConfigFile(e, KEYSTORE_FILE, ksPath);
        } catch (AccessControlException e) {
            throw blockedTrustConfigFile(e, environment, KEYSTORE_FILE, List.of(ksPath));
        } catch (IOException | GeneralSecurityException e) {
            throw new ElasticsearchException("failed to initialize SSL TrustManager", e);
        }
    }

    @Override
    Collection<CertificateInfo> certificates(Environment environment) throws GeneralSecurityException, IOException {
        final KeyStore trustStore = getStore(CertParsingUtils.resolvePath(keyStorePath, environment), keyStoreType, keyStorePassword);
        final List<CertificateInfo> certificates = new ArrayList<>();
        final Enumeration<String> aliases = trustStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            final Certificate[] chain = trustStore.getCertificateChain(alias);
            if (chain == null) {
                continue;
            }
            for (int i = 0; i < chain.length; i++) {
                final Certificate certificate = chain[i];
                if (certificate instanceof X509Certificate) {
                    certificates.add(new CertificateInfo(keyStorePath, keyStoreType, alias, i == 0, (X509Certificate) certificate));
                }
            }
        }
        return certificates;
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        return Collections.singletonList(CertParsingUtils.resolvePath(keyStorePath, environment));
    }

    @Override
    List<PrivateKey> privateKeys(@Nullable Environment environment) {
        try {
            KeyStore keyStore = getStore(environment, keyStorePath, keyStoreType, keyStorePassword);
            List<PrivateKey> privateKeys = new ArrayList<>();
            for (Enumeration<String> e = keyStore.aliases(); e.hasMoreElements(); ) {
                final String alias = e.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    Key key = keyStore.getKey(alias, keyPassword.getChars());
                    if (key instanceof PrivateKey) {
                        privateKeys.add((PrivateKey) key);
                    }
                }
            }
            return privateKeys;
        } catch (Exception e) {
            throw new ElasticsearchException("failed to list keys", e);
        }
    }

    public List<Tuple<PrivateKey, X509Certificate>> getPrivateKeyEntries(Environment environment) {
        try {
            final KeyStore keyStore = getStore(CertParsingUtils.resolvePath(keyStorePath, environment), keyStoreType, keyStorePassword);
            List<Tuple<PrivateKey, X509Certificate>> entries = new ArrayList<>();
            for (Enumeration<String> e = keyStore.aliases(); e.hasMoreElements(); ) {
                final String alias = e.nextElement();
                if (keyStore.isKeyEntry(alias)) {
                    Key key = keyStore.getKey(alias, keyPassword.getChars());
                    Certificate certificate = keyStore.getCertificate(alias);
                    if (key instanceof PrivateKey && certificate instanceof X509Certificate) {
                        entries.add(Tuple.tuple((PrivateKey) key, (X509Certificate) certificate));
                    }
                }
            }
            return entries;
        } catch (Exception e) {
            throw new ElasticsearchException("failed to list keys and certificates", e);
        }
    }

    private void checkKeyStore(KeyStore keyStore) throws KeyStoreException {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                return;
            }
        }
        throw new IllegalArgumentException("the keystore [" + keyStorePath + "] does not contain a private key entry");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoreKeyConfig that = (StoreKeyConfig) o;
        return Objects.equals(keyStorePath, that.keyStorePath)
            && Objects.equals(keyStoreType, that.keyStoreType)
            && Objects.equals(keyStorePassword, that.keyStorePassword)
            && Objects.equals(keyStoreAlgorithm, that.keyStoreAlgorithm)
            && Objects.equals(keyPassword, that.keyPassword)
            && Objects.equals(trustStoreAlgorithm, that.trustStoreAlgorithm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyStorePath, keyStoreType, keyStorePassword, keyStoreAlgorithm, keyPassword, trustStoreAlgorithm);
    }

    @Override
    public String toString() {
        return "keyStorePath=[" + keyStorePath +
                "], keyStoreType=[" + keyStoreType +
                "], keyStoreAlgorithm=[" + keyStoreAlgorithm +
                "], trustStoreAlgorithm=[" + trustStoreAlgorithm +
                "]";
    }
}
