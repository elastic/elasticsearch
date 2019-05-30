/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

/**
 * The type of a TLS message (see {@link TlsPlaintext#getContentType()}).
 */
enum TlsContentType {
    ChangeCipherSpec((byte) 20),
    Alert((byte) 21),
    Handshake((byte) 22),
    ApplicationData((byte) 23);

    private final byte type;

    TlsContentType(byte type) {
        this.type = type;
    }

    static TlsContentType forValue(byte value) {
        switch (value) {
            case 20:
                return TlsContentType.ChangeCipherSpec;
            case 21:
                return TlsContentType.Alert;
            case 22:
                return TlsContentType.Handshake;
            case 23:
                return TlsContentType.ApplicationData;
            default:
                throw new IllegalArgumentException("TLS Content Type value of " + value + " is not valid.");
        }
    }
}
