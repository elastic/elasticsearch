/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

@SuppressForbidden(reason = "Implements com.sun class")
public class PemHttpsConfigurator extends com.sun.net.httpserver.HttpsConfigurator {

    public PemHttpsConfigurator(Path certificate, Path key, char[] keyPassword) throws GeneralSecurityException, IOException {
        super(buildContext(certificate, key, keyPassword));
    }

    private static SSLContext buildContext(Path certPath, Path keyPath, char[] keyPassword) throws GeneralSecurityException, IOException {
        final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        final PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, () -> keyPassword);
        final List<Certificate> certificates = PemUtils.readCertificates(List.of(certPath));
        final KeyManager keyManager = KeyStoreUtil.createKeyManager(certificates.toArray(Certificate[]::new), privateKey, keyPassword);
        sslContext.init(new KeyManager[] { keyManager }, null, null);
        return sslContext;
    }
}
