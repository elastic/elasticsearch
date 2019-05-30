/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

/**
 * The type of a TLS handshake message (see {@link TlsHandshake#type()})
 */
enum TlsHandshakeType {
    HelloRequest((byte) 0),
    ClientHello((byte) 1),
    ServerHello((byte) 2),
    Certificate((byte) 11),
    ServerKeyExchange((byte) 12),
    CertificateRequest((byte) 13),
    ServerHelloDone((byte) 14),
    CertificateVerify((byte) 15),
    ClientKeyExchange((byte) 16),
    Finished((byte) 20);

    private final byte type;

    TlsHandshakeType(byte type) {
        this.type = type;
    }

    static TlsHandshakeType forValue(byte value) {
        switch (value) {
            case 0:
                return TlsHandshakeType.HelloRequest;
            case 1:
                return TlsHandshakeType.ClientHello;
            case 2:
                return TlsHandshakeType.ServerHello;
            case 11:
                return TlsHandshakeType.Certificate;
            case 12:
                return TlsHandshakeType.ServerKeyExchange;
            case 13:
                return TlsHandshakeType.CertificateRequest;
            case 14:
                return TlsHandshakeType.ServerHelloDone;
            case 15:
                return TlsHandshakeType.CertificateVerify;
            case 16:
                return TlsHandshakeType.ClientKeyExchange;
            case 20:
                return TlsHandshakeType.Finished;
            default:
                throw new IllegalArgumentException("HandshakeType value of " + value + " is not valid.");
        }
    }
}
