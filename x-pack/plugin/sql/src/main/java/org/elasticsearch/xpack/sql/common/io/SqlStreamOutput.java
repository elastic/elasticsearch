/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.common.io;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;

/**
 * Output stream for writing SQL cursors. The output is compressed if it would become larger than {@code compressionThreshold}
 * bytes otherwise (see {@code DEFAULT_COMPRESSION_THRESHOLD}).
 *
 * The wire format is {@code version compressedPayload}.
 */
public class SqlStreamOutput extends OutputStreamStreamOutput {

    private final ByteArrayOutputStream bytes;

    public static SqlStreamOutput create(TransportVersion version, ZoneId zoneId) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamOutput uncompressedOut = new OutputStreamStreamOutput(Base64.getEncoder().wrap(bytes));
        TransportVersion.writeVersion(version, uncompressedOut);
        OutputStream out = CompressorFactory.COMPRESSOR.threadLocalOutputStream(uncompressedOut);
        return new SqlStreamOutput(bytes, out, version, zoneId);
    }

    private SqlStreamOutput(ByteArrayOutputStream bytes, OutputStream out, TransportVersion version, ZoneId zoneId) throws IOException {
        super(out);
        this.bytes = bytes;
        super.setTransportVersion(version);
        this.writeZoneId(zoneId);
    }

    /**
     * Should be called _after_ closing the stream - there are no guarantees otherwise.
     */
    public String streamAsString() throws IOException {
        return bytes.toString(StandardCharsets.ISO_8859_1);
    }

}
