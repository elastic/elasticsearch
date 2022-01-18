/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.common.ssl.PemUtils;

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

        final List<Certificate> certificates = PemUtils.readCertificates(List.of(certPath));
        if (certificates.isEmpty()) {
            throw new IllegalArgumentException("No certificates found in " + certPath);
        }
        final PrivateKey key = PemUtils.readPrivateKey(keyPath, () -> password);

        KeyStore keyStore = KeyStore.getInstance(keystoreType);
        keyStore.load(null);
        keyStore.setKeyEntry("key", key, password, certificates.toArray(Certificate[]::new));
        try (OutputStream out = Files.newOutputStream(keystorePath)) {
            keyStore.store(out, password);
        }
    }

}
