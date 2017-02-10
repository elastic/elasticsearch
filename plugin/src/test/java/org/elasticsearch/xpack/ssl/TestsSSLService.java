/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.bouncycastle.operator.OperatorCreationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import javax.net.ssl.SSLContext;
import javax.security.auth.DestroyFailedException;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

/**
 * Extending SSLService to make helper methods public to access in tests
 */
public class TestsSSLService extends SSLService {

    public TestsSSLService(Settings settings, Environment environment) throws CertificateException, UnrecoverableKeyException,
            NoSuchAlgorithmException, IOException, DestroyFailedException, KeyStoreException, OperatorCreationException {
        super(settings, environment);
    }

    @Override
    public SSLContext sslContext() {
        return super.sslContext();
    }

    /**
     * Allows to get alternative ssl context, like for the http client
     */
    public SSLContext sslContext(Settings settings) {
        return sslContextHolder(super.sslConfiguration(settings)).sslContext();
    }
}
