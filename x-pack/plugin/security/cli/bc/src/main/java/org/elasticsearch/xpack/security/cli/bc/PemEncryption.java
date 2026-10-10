/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli.bc;

/**
 * The OpenSSL-style ({@code DEK-Info}) encryption algorithms security-cli uses when writing password protected PEM private keys.
 */
public enum PemEncryption {
    AES_128_CBC("AES-128-CBC"),
    DES_EDE3_CBC("DES-EDE3-CBC");

    private final String algorithm;

    PemEncryption(String algorithm) {
        this.algorithm = algorithm;
    }

    String algorithm() {
        return algorithm;
    }
}
