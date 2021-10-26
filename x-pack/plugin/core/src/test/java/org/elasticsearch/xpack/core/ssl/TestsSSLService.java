/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import javax.net.ssl.SSLContext;

/**
 * Extending SSLService to make helper methods public to access in tests
 */
public class TestsSSLService extends SSLService {

    public TestsSSLService(Environment environment) {
        super(environment);
    }

    /**
     * Allows to get alternative ssl context, like for the http client
     */
    public SSLContext sslContext(Settings settings) {
        return sslContextHolder(super.sslConfiguration(settings)).sslContext();
    }

    public SSLContext sslContext(String context) {
        return sslContextHolder(super.getSSLConfiguration(context)).sslContext();
    }
}
