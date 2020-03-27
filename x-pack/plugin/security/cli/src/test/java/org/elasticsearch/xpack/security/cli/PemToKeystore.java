/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;
import org.elasticsearch.xpack.core.ssl.PemUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.List;

@SuppressForbidden(reason = "CLI utility for testing only")
public class PemToKeystore {

    public static void main(String[] args) throws IOException, GeneralSecurityException {
        if (args.length != 5) {
            System.out.println("Usage: <java> " + PemToKeystore.class.getName() + " <keystore> <PKCS12|jks> <cert> <key> <password>");
            return;
        }
        Path keystorePath = Paths.get(args[0]).toAbsolutePath();
        String keystoreType = args[1];
        Path certPath = Paths.get(args[2]).toAbsolutePath();
        Path keyPath = Paths.get(args[3]).toAbsolutePath();
        char[] password = args[4].toCharArray();

        final Certificate[] certificates = CertParsingUtils.readCertificates(List.of(certPath));
        if (certificates.length == 0) {
            throw new IllegalArgumentException("No certificates found in " + certPath);
        }
        final PrivateKey key = PemUtils.readPrivateKey(keyPath, () -> password);

        KeyStore keyStore = KeyStore.getInstance(keystoreType);
        keyStore.load(null);
        keyStore.setKeyEntry("key", key, password, certificates);
        try (OutputStream out = Files.newOutputStream(keystorePath)) {
            keyStore.store(out, password);
        }
    }

}
