/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.api;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

/**
 * todo - probably makes sense to encode in two separate byte arrays - one for IP and one for port
 *
 * Represents an IPv4 address with port argument extracted from a text message.
 * <p>
 * The value is a byte array containing the four octets of the IPv4 address followed by
 * two bytes for the port number (little-endian encoding).
 * Format: [octet0, octet1, octet2, octet3, port_low_byte, port_high_byte]
 */
public final class IPv4AddressArgument extends ByteEncodedArgument {

    private final int port;

    public IPv4AddressArgument(int startPosition, int length, int[] octets, int firstOctetIndex, int port) {
        super(startPosition, length, 6);

        for (int i = firstOctetIndex; i < 4; i++) {
            encodedBytes[i] = (byte) octets[i];
        }
        this.port = port;

        // Encode port in little-endian format (2 bytes)
        encodedBytes[4] = (byte) (port & 0xFF);        // low byte
        encodedBytes[5] = (byte) ((port >> 8) & 0xFF); // high byte
    }

    @Override
    public EncodingType type() {
        return EncodingType.IPV4_ADDRESS;
    }

    public int getPort() {
        return port;
    }
}
