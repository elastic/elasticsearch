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

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.UnrecoverableKeyException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;

/**
 * A {@link SslKeyConfig} that builds a Key Manager from a keystore file.
 */
public class StoreKeyConfig implements SslKeyConfig {
    private final Path path;
    private final char[] storePassword;
    private final String type;
    private final char[] keyPassword;
    private final String algorithm;

    /**
     * @param path          The path to the keystore file
     * @param storePassword The password for the keystore
     * @param type          The {@link KeyStore#getType() type} of the keystore (typically "PKCS12" or "jks").
     *                      See {@link KeyStoreUtil#inferKeyStoreType(Path)}.
     * @param keyPassword   The password for the key(s) within the keystore
     *                      (see {@link javax.net.ssl.KeyManagerFactory#init(KeyStore, char[])}).
     * @param algorithm     The algorithm to use for the Key Manager (see {@link KeyManagerFactory#getAlgorithm()}).
     */
    StoreKeyConfig(Path path, char[] storePassword, String type, char[] keyPassword, String algorithm) {
        this.path = path;
        this.storePassword = storePassword;
        this.type = type;
        this.keyPassword = keyPassword;
        this.algorithm = algorithm;
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return Collections.singleton(path);
    }

    @Override
    public X509ExtendedKeyManager createKeyManager() {
        try {
            final KeyStore keyStore = KeyStoreUtil.readKeyStore(path, type, storePassword);
            checkKeyStore(keyStore);
            return KeyStoreUtil.createKeyManager(keyStore, keyPassword, algorithm);
        } catch (UnrecoverableKeyException e) {
            String message = "failed to load a KeyManager for keystore [" + path.toAbsolutePath()
                + "], this is usually caused by an incorrect key-password";
            if (keyPassword.length == 0) {
                message += " (no key-password was provided)";
            } else if (Arrays.equals(storePassword, keyPassword)) {
                message += " (we tried to access the key using the same password as the keystore)";
            }
            throw new SslConfigException(message, e);
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("failed to load a KeyManager for keystore [" + path + "] of type [" + type + "]", e);
        }
    }

    /**
     * Verifies that the keystore contains at least 1 private key entry.
     */
    private void checkKeyStore(KeyStore keyStore) throws KeyStoreException {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                return;
            }
        }
        final String message;
        if (path != null) {
            message = "the keystore [" + path + "] does not contain a private key entry";
        } else {
            message = "the configured PKCS#11 token does not contain a private key entry";
        }
        throw new SslConfigException(message);
    }

}
