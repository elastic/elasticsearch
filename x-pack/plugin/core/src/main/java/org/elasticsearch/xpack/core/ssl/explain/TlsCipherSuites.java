/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

import javax.net.ssl.SSLContext;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents a collection of TLS Cipher Suites in raw form.
 */
class TlsCipherSuites {

    static final List<String> JVM_SUPPORTED_CIPHER_SUITES;

    static {
        List<String> ciphers;
        try {
            ciphers = Arrays.asList(SSLContext.getDefault().getSupportedSSLParameters().getCipherSuites());
        } catch (NoSuchAlgorithmException e) {
            // This should't happen...
            ciphers = Collections.emptyList();
        }
        JVM_SUPPORTED_CIPHER_SUITES = ciphers;
    }

    private final byte[] suites;

    private TlsCipherSuites(byte[] suites) {
        // Don't decipher these now because we probably won't do anything with them.
        // Just store the byte codes for future reference.
        this.suites = suites;
    }

    /**
     * Returns the name of the cipher suites that are held by this object.
     */
    public List<String> names() {
        final List<String> names = new ArrayList<>(suites.length / 2);
        for (int i = 0; i < suites.length; i += 2) {
            names.add(CipherSuiteNames.nameFor(suites[i], suites[i + 1]));
        }
        return names;
    }

    public static TlsCipherSuites parse(ByteBuffer buffer) {
        int cipherSuitesLength = BufferUtil.readUInt16(buffer);
        return new TlsCipherSuites(BufferUtil.readByteArray(buffer, cipherSuitesLength));
    }

}
