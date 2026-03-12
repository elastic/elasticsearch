/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.util.Maps;
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
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.net.ssl.KeyManagerFactory;
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

    public static X509Certificate readX509Certificate(Path path) throws CertificateException, IOException {
        List<Certificate> certificates = PemUtils.readCertificates(List.of(path));
        if (certificates.size() != 1) {
            throw new IllegalArgumentException(
                "expected a single certificate in file [" + path.toAbsolutePath() + "] but found [" + certificates.size() + "]"
            );
        }
        final Certificate cert = certificates.get(0);
        if (cert instanceof X509Certificate) {
            return (X509Certificate) cert;
        } else {
            throw new IllegalArgumentException(
                "the certificate in "
                    + path.toAbsolutePath()
                    + " is not an X.509 certificate ("
                    + cert.getType()
                    + " : "
                    + cert.getClass()
                    + ")"
            );
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

    public static Map<Certificate, Key> readKeyPairsFromKeystore(
        Path path,
        String storeType,
        char[] password,
        Function<String, char[]> keyPassword
    ) throws IOException, GeneralSecurityException {
        final KeyStore store = KeyStoreUtil.readKeyStore(path, storeType, password);
        return readKeyPairsFromKeystore(store, keyPassword);
    }

    private static Map<Certificate, Key> readKeyPairsFromKeystore(KeyStore store, Function<String, char[]> keyPassword)
        throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException {
        final Enumeration<String> enumeration = store.aliases();
        final Map<Certificate, Key> map = Maps.newMapWithExpectedSize(store.size());
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
    public static X509ExtendedKeyManager getKeyManagerFromPEM(Path certificatePath, Path keyPath, char[] keyPassword) throws IOException,
        GeneralSecurityException {
        final KeyStore keyStore = getKeyStoreFromPEM(certificatePath, keyPath, keyPassword);
        return KeyStoreUtil.createKeyManager(keyStore, keyPassword, KeyManagerFactory.getDefaultAlgorithm());
    }

    public static SslKeyConfig createKeyConfig(
        Settings settings,
        String prefix,
        Environment environment,
        boolean acceptNonSecurePasswords
    ) {
        final SslSettingsLoader settingsLoader = new SslSettingsLoader(settings, prefix, acceptNonSecurePasswords);
        return settingsLoader.buildKeyConfig(environment.configDir());
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the provided PEM certificate authorities
     */
    public static X509ExtendedTrustManager getTrustManagerFromPEM(List<Path> caPaths) throws GeneralSecurityException, IOException {
        final List<Certificate> certificates = PemUtils.readCertificates(caPaths);
        return KeyStoreUtil.createTrustManager(certificates);
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
