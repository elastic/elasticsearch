/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

/** Miscellaneous utulity methods for reading certificates and keystores.
 * @see KeyStoreUtil
 * @see PemUtils
  */
public class CertParsingUtils {

    private CertParsingUtils() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }

    static Path resolvePath(String path, Environment environment) {
        return environment.configFile().resolve(path);
    }

    static List<Path> resolvePaths(List<String> certPaths, Environment environment) {
        return certPaths.stream().map(p -> environment.configFile().resolve(p)).collect(Collectors.toList());
    }

    /**
     * Reads the provided paths and parses them into {@link Certificate} objects
     *
     * @param certPaths   the paths to the PEM encoded certificates
     * @param environment the environment to resolve files against. May be not be {@code null}
     * @return an array of {@link Certificate} objects
     */
    public static Certificate[] readCertificates(List<String> certPaths, Environment environment)
        throws CertificateException, IOException {
        final List<Path> resolvedPaths = resolvePaths(certPaths, environment);
        return readX509Certificates(resolvedPaths);
    }

    public static X509Certificate readX509Certificate(Path path) throws CertificateException, IOException {
        List<Certificate> certificates = PemUtils.readCertificates(List.of(path));
        if (certificates.size() != 1) {
            throw new IllegalArgumentException("expected a single certificate in file [" + path.toAbsolutePath() + "] but found [" +
                certificates.size() + "]");
        }
        final Certificate cert = certificates.get(0);
        if (cert instanceof X509Certificate) {
            return (X509Certificate) cert;
        } else {
            throw new IllegalArgumentException("the certificate in " + path.toAbsolutePath() + " is not an X.509 certificate ("
                + cert.getType()
                + " : "
                + cert.getClass() + ")");
        }
    }

    @SuppressWarnings("unchecked")
    public static X509Certificate[] readX509Certificates(List<Path> certPaths) throws CertificateException, IOException {
        return PemUtils.readCertificates(certPaths).stream().map(X509Certificate.class::cast).toArray(X509Certificate[]::new);
    }

    public static List<Certificate> readCertificates(InputStream input) throws CertificateException, IOException {
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        Collection<? extends Certificate> certificates = certFactory.generateCertificates(input);
        return new ArrayList<>(certificates);
    }

    /**
     * Read all certificate-key pairs from a PKCS#12 container.
     *
     * @param path        The path to the PKCS#12 container file.
     * @param password    The password for the container file
     * @param keyPassword A supplier for the password for each key. The key alias is supplied as an argument to the function, and it should
     *                    return the password for that key. If it returns {@code null}, then the key-pair for that alias is not read.
     */
    public static Map<Certificate, Key> readPkcs12KeyPairs(Path path, char[] password, Function<String, char[]> keyPassword)
        throws GeneralSecurityException, IOException {
        return readKeyPairsFromKeystore(path, "PKCS12", password, keyPassword);
    }

    public static Map<Certificate, Key> readKeyPairsFromKeystore(Path path, String storeType, char[] password,
                                                                  Function<String, char[]> keyPassword)
        throws IOException, GeneralSecurityException {
        final KeyStore store = KeyStoreUtil.readKeyStore(path, storeType, password);
        return readKeyPairsFromKeystore(store, keyPassword);
    }

    private static Map<Certificate, Key> readKeyPairsFromKeystore(KeyStore store, Function<String, char[]> keyPassword)
        throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException {
        final Enumeration<String> enumeration = store.aliases();
        final Map<Certificate, Key> map = new HashMap<>(store.size());
        while (enumeration.hasMoreElements()) {
            final String alias = enumeration.nextElement();
            if (store.isKeyEntry(alias)) {
                final char[] pass = keyPassword.apply(alias);
                map.put(store.getCertificate(alias), store.getKey(alias, pass));
            }
        }
        return map;
    }

    /**
     * Creates a {@link KeyStore} from a PEM encoded certificate and key file
     */
    public static KeyStore getKeyStoreFromPEM(Path certificatePath, Path keyPath, char[] keyPassword) throws IOException,
        GeneralSecurityException {
        final PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, () -> keyPassword);
        final List<Certificate> certificates = PemUtils.readCertificates(List.of(certificatePath));
        return KeyStoreUtil.buildKeyStore(certificates, privateKey, keyPassword);
    }

    /**
     * Creates a {@link X509ExtendedKeyManager} from a PEM encoded certificate and key file
     */
    public static X509ExtendedKeyManager getKeyManagerFromPEM(Path certificatePath, Path keyPath, char[] keyPassword)
        throws IOException, GeneralSecurityException {
        final KeyStore keyStore = getKeyStoreFromPEM(certificatePath, keyPath, keyPassword);
        return KeyStoreUtil.createKeyManager(keyStore, keyPassword, KeyManagerFactory.getDefaultAlgorithm());
    }

    /**
     * Returns a {@link X509ExtendedKeyManager} that is built from the provided private key and certificate chain
     */
    public static X509ExtendedKeyManager keyManager(Certificate[] certificateChain, PrivateKey privateKey, char[] password)
        throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException, IOException, CertificateException {
        KeyStore keyStore = getKeyStore(certificateChain, privateKey, password);
        return keyManager(keyStore, password, KeyManagerFactory.getDefaultAlgorithm());
    }

    public static KeyStore getKeyStore(Certificate[] certificateChain, PrivateKey privateKey, char[] password)
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        // password must be non-null for keystore...
        keyStore.setKeyEntry("key", privateKey, password, certificateChain);
        return keyStore;
    }

    /**
     * Returns a {@link X509ExtendedKeyManager} that is built from the provided keystore
     */
    public static X509ExtendedKeyManager keyManager(KeyStore keyStore, char[] password, String algorithm)
        throws NoSuchAlgorithmException, UnrecoverableKeyException, KeyStoreException {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
        kmf.init(keyStore, password);
        KeyManager[] keyManagers = kmf.getKeyManagers();
        for (KeyManager keyManager : keyManagers) {
            if (keyManager instanceof X509ExtendedKeyManager) {
                return (X509ExtendedKeyManager) keyManager;
            }
        }
        throw new IllegalStateException("failed to find a X509ExtendedKeyManager");
    }

    static KeyConfig createKeyConfig(X509KeyPairSettings keyPair, Settings settings, String trustStoreAlgorithm) {
        String keyPath = keyPair.keyPath.get(settings).orElse(null);
        String keyStorePath = keyPair.keystorePath.get(settings).orElse(null);
        String keyStoreType = SSLConfigurationSettings.getKeyStoreType(keyPair.keystoreType, settings, keyStorePath);

        if (keyPath != null && keyStorePath != null) {
            throw new IllegalArgumentException("you cannot specify a keystore and key file");
        }

        if (keyPath != null) {
            SecureString keyPassword = keyPair.keyPassword.get(settings);
            String certPath = keyPair.certificatePath.get(settings).orElse(null);
            if (certPath == null) {
                throw new IllegalArgumentException("you must specify the certificates [" + keyPair.certificatePath.getKey()
                    + "] to use with the key [" + keyPair.keyPath.getKey() + "]");
            }
            return new PEMKeyConfig(keyPath, keyPassword, certPath);
        }

        if (keyStorePath != null) {
            SecureString keyStorePassword = keyPair.keystorePassword.get(settings);
            String keyStoreAlgorithm = keyPair.keystoreAlgorithm.get(settings);
            SecureString keyStoreKeyPassword = keyPair.keystoreKeyPassword.get(settings);
            if (keyStoreKeyPassword.length() == 0) {
                keyStoreKeyPassword = keyStorePassword;
            }
            return new StoreKeyConfig(
                keyStorePath,
                keyStoreType,
                keyStorePassword,
                keyStoreKeyPassword,
                keyStoreAlgorithm,
                trustStoreAlgorithm
            );
        }
        return null;
    }

    public static SslKeyConfig createKeyConfig(Settings settings, String prefix, Environment environment,
                                               boolean acceptNonSecurePasswords) {
        final SslSettingsLoader settingsLoader = new SslSettingsLoader(settings, prefix, acceptNonSecurePasswords);
        return settingsLoader.buildKeyConfig(environment.configFile());
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the provided PEM certificate authorities
     */
    public static X509ExtendedTrustManager getTrustManagerFromPEM(List<Path> caPaths) throws GeneralSecurityException, IOException {
        final List<Certificate> certificates = PemUtils.readCertificates(caPaths);
        return KeyStoreUtil.createTrustManager(certificates);
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the provided certificates
     *
     * @param certificates the certificates to trust
     * @return a trust manager that trusts the provided certificates
     */
    public static X509ExtendedTrustManager trustManager(Certificate[] certificates)
        throws NoSuchAlgorithmException, KeyStoreException, IOException, CertificateException {
        KeyStore store = trustStore(certificates);
        return trustManager(store, TrustManagerFactory.getDefaultAlgorithm());
    }

    public static KeyStore trustStore(Certificate[] certificates)
        throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        assert certificates != null : "Cannot create trust store with null certificates";
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
        store.load(null, null);
        int counter = 0;
        for (Certificate certificate : certificates) {
            store.setCertificateEntry("cert" + counter, certificate);
            counter++;
        }
        return store;
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the trust material in the provided {@link KeyStore}
     */
    public static X509ExtendedTrustManager trustManager(KeyStore keyStore, String algorithm)
        throws NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
        tmf.init(keyStore);
        TrustManager[] trustManagers = tmf.getTrustManagers();
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509ExtendedTrustManager) {
                return (X509ExtendedTrustManager) trustManager;
            }
        }
        throw new IllegalStateException("failed to find a X509ExtendedTrustManager");
    }

    /**
     * Checks that the {@code X509Certificate} array is ordered, such that the end-entity certificate is first and it is followed by any
     * certificate authorities'. The check validates that the {@code issuer} of every certificate is the {@code subject} of the certificate
     * in the next array position. No other certificate attributes are checked.
     */
    public static boolean isOrderedCertificateChain(List<X509Certificate> chain) {
        for (int i = 1; i < chain.size(); i++) {
            X509Certificate cert = chain.get(i - 1);
            X509Certificate issuer = chain.get(i);
            if (false == cert.getIssuerX500Principal().equals(issuer.getSubjectX500Principal())) {
                return false;
            }
        }
        return true;
    }
}
