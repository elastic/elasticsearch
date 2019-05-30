/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ssl.explain;

import org.elasticsearch.common.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A SSL/TLS Plaintext message. Cloned from <a href="https://github.com/elastic/tealess">Tealess</a>.
 */
class TlsPlaintext {

    private static final int BYTES_CONTENT_TYPE = 1;
    private static final int BYTES_VERSION = 2;
    private static final int BYTES_LENGTH = 2;
    // In order to read a TLS message we must have bytes for the 3 headers
    // Application Data messages may be zero-length, so it is possible to have a plain text message with only header data
    private static final int MIN_READABLE_BYTES = BYTES_CONTENT_TYPE + BYTES_VERSION + BYTES_LENGTH;
    private static final byte[] EMPTY_BYTES = new byte[0];

    private final int length; // length is uint16 in the spec, but Java has no unsigned types, so we use int.
    private final ByteBuffer payload;
    private final TlsContentType contentType;
    private final TlsVersion version;

    private TlsPlaintext(TlsContentType contentType, TlsVersion version, int length, ByteBuffer payload) {
        this.contentType = contentType;
        this.version = version;
        this.length = length;
        this.payload = payload;
    }

    /**
     * Parse a TLS message from the supplied buffer, iff the buffer contains a complete message. Otherwise returns null.
     */
    @Nullable
    public static TlsPlaintext parse(ByteBuffer buffer) {
        buffer.order(ByteOrder.BIG_ENDIAN);
        if (buffer.remaining() < MIN_READABLE_BYTES) {
            return null;
        }

        buffer.mark();
        byte contentTypeByte = buffer.get();
        TlsContentType contentType = TlsContentType.forValue(contentTypeByte);
        TlsVersion version = TlsVersion.forValue(buffer.get(), buffer.get());

        int length = buffer.getShort() & 0xffff;
        // RFC: The length MUST NOT exceed 2^14 bytes, but may be zero (for Application Data only)
        assert (length <= 1 << 14);

        if (length == 0) {
            return new TlsPlaintext(contentType, version, length, ByteBuffer.wrap(EMPTY_BYTES));
        }
        if (buffer.remaining() < length) {
            buffer.reset();
            return null;
        }
        ByteBuffer payload = buffer.duplicate();
        payload.limit(buffer.position() + length);
        buffer.position(payload.limit());
        return new TlsPlaintext(contentType, version, length, payload);
    }

    public String toString() {
        return "TLSPlaintext[" + contentType + ", " + version + ", length:" + length + "]";
    }

    public TlsContentType getContentType() {
        return contentType;
    }

    public ByteBuffer getPayload() {
        return payload;
    }
}
