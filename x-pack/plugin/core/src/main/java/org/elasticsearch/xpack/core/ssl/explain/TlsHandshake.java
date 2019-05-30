/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

import java.nio.ByteBuffer;

/**
 * A base class for TLS handshake messages (see {@link TlsContentType#Handshake}).
 */
public abstract class TlsHandshake {

    public static TlsHandshake parse(ByteBuffer buffer) {
        TlsHandshakeType handshakeType = TlsHandshakeType.forValue(buffer.get());
        int length = BufferUtil.readUInt24(buffer);

        assert buffer.remaining() == length;

        switch (handshakeType) {
            case ClientHello:
                return parseClientHello(buffer, length);
            default:
                return null;
        }
    }

    public abstract TlsHandshakeType type();

    private static ClientHello parseClientHello(ByteBuffer buffer, int length) {
        TlsVersion version = TlsVersion.forValue(buffer.get(), buffer.get());

        // Random...
        //noinspection unused
        long time = BufferUtil.readUInt32(buffer);
        //noinspection unused
        byte[] random = BufferUtil.readByteArray(buffer, 28);
        byte[] session = getSessionID(buffer);

        final TlsCipherSuites cipherSuites = TlsCipherSuites.parse(buffer);

        int compressionMethodsLength = BufferUtil.readUInt8(buffer);
        BufferUtil.skip(buffer, compressionMethodsLength);

        // Per RFC, older clients may not support extensions and thus won't send any.
        if (buffer.hasRemaining()) {
            final int extensionsLength = BufferUtil.readUInt16(buffer);
            BufferUtil.skip(buffer, extensionsLength);
        }

        return new ClientHello(version, session, cipherSuites);
    }

    private static byte[] getSessionID(ByteBuffer buffer) {
        int sessionLength = buffer.get();
        return BufferUtil.readByteArray(buffer, sessionLength);
    }

    public static class ClientHello extends TlsHandshake {
        private final TlsVersion version;
        private final byte[] session;
        private final TlsCipherSuites cipherSuites;

        public ClientHello(TlsVersion version, byte[] session, TlsCipherSuites cipherSuites) {
            this.version = version;
            this.session = session;
            this.cipherSuites = cipherSuites;
        }

        @Override
        public TlsHandshakeType type() {
            return TlsHandshakeType.ClientHello;
        }

        public TlsCipherSuites cipherSuites() {
            return cipherSuites;
        }
    }
}
