/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.common.Nullable;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Collection;
import java.util.Locale;

/**
 * A variety of utility methods for working with or constructing {@link KeyStore} instances.
 */
final class KeyStoreUtil {

    private KeyStoreUtil() {
        throw new IllegalStateException("Utility class should not be instantiated");
    }

    /**
     * Make a best guess about the "type" (see {@link KeyStore#getType()}) of the keystore file located at the given {@code Path}.
     * This method only references the <em>file name</em> of the keystore, it does not look at its contents.
     */
    static String inferKeyStoreType(Path path) {
        String name = path == null ? "" : path.toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".p12") || name.endsWith(".pfx") || name.endsWith(".pkcs12")) {
            return "PKCS12";
        } else {
            return "jks";
        }
    }

    /**
     * Read the given keystore file.
     *
     * @throws SslConfigException       If there is a problem reading from the provided path
     * @throws GeneralSecurityException If there is a problem with the keystore contents
     */
    static KeyStore readKeyStore(Path path, String type, char[] password) throws GeneralSecurityException {
        if (Files.notExists(path)) {
            throw new SslConfigException("cannot read a [" + type + "] keystore from [" + path.toAbsolutePath()
                + "] because the file does not exist");
        }
        try {
            KeyStore keyStore = KeyStore.getInstance(type);
            try (InputStream in = Files.newInputStream(path)) {
                keyStore.load(in, password);
            }
            return keyStore;
        } catch (IOException e) {
            throw new SslConfigException("cannot read a [" + type + "] keystore from [" + path.toAbsolutePath() + "] - " + e.getMessage(),
                e);
        }
    }

    /**
     * Construct an in-memory keystore with a single key entry.
     * @param certificateChain A certificate chain (ordered from subject to issuer)
     * @param privateKey The private key that corresponds to the subject certificate (index 0 of {@code certificateChain})
     * @param password The password for the private key
     *
     * @throws GeneralSecurityException If there is a problem with the provided certificates/key
     */
    static KeyStore buildKeyStore(Collection<Certificate> certificateChain, PrivateKey privateKey, char[] password)
        throws GeneralSecurityException {
        KeyStore keyStore = buildNewKeyStore();
        keyStore.setKeyEntry("key", privateKey, password, certificateChain.toArray(new Certificate[0]));
        return keyStore;
    }

    /**
     * Construct an in-memory keystore with multiple trusted cert entries.
     * @param certificates The root certificates to trust
     */
    static KeyStore buildTrustStore(Iterable<Certificate> certificates) throws GeneralSecurityException {
        assert certificates != null : "Cannot create keystore with null certificates";
        KeyStore store = buildNewKeyStore();
        int counter = 0;
        for (Certificate certificate : certificates) {
            store.setCertificateEntry("cert-" + counter, certificate);
            counter++;
        }
        return store;
    }

    private static KeyStore buildNewKeyStore() throws GeneralSecurityException {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            keyStore.load(null, null);
        } catch (IOException e) {
            // This should never happen so callers really shouldn't be forced to deal with it themselves.
            throw new SslConfigException("Unexpected error initializing a new in-memory keystore", e);
        }
        return keyStore;
    }

    /**
     * Creates a {@link X509ExtendedKeyManager} based on the key material in the provided {@link KeyStore}
     */
    static X509ExtendedKeyManager createKeyManager(KeyStore keyStore, char[] password, String algorithm) throws GeneralSecurityException {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
        kmf.init(keyStore, password);
        KeyManager[] keyManagers = kmf.getKeyManagers();
        for (KeyManager keyManager : keyManagers) {
            if (keyManager instanceof X509ExtendedKeyManager) {
                return (X509ExtendedKeyManager) keyManager;
            }
        }
        throw new SslConfigException("failed to find a X509ExtendedKeyManager in the key manager factory for [" + algorithm
            + "] and keystore [" + keyStore + "]");
    }

    /**
     * Creates a {@link X509ExtendedTrustManager} based on the trust material in the provided {@link KeyStore}
     */
    static X509ExtendedTrustManager createTrustManager(@Nullable KeyStore trustStore, String algorithm)
        throws NoSuchAlgorithmException, KeyStoreException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
        tmf.init(trustStore);
        TrustManager[] trustManagers = tmf.getTrustManagers();
        for (TrustManager trustManager : trustManagers) {
            if (trustManager instanceof X509ExtendedTrustManager) {
                return (X509ExtendedTrustManager) trustManager;
            }
        }
        throw new SslConfigException("failed to find a X509ExtendedTrustManager in the trust manager factory for [" + algorithm
            + "] and truststore [" + trustStore + "]");
    }


}
