/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.common.io;

import org.elasticsearch.Version;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneId;
import java.util.Base64;

/**
 * Output stream for writing SQL cursors. The output is compressed if it would become larger than {@code compressionThreshold}
 * bytes otherwise (see {@code DEFAULT_COMPRESSION_THRESHOLD}).
 */
public class SqlStreamOutput extends StreamOutput {

    public static final byte HEADER_UNCOMPRESSED = 0;
    public static final byte HEADER_COMPRESSED = 1;
    private static final int DEFAULT_COMPRESSION_THRESHOLD = 1000;

    private ByteArrayOutputStream bytes;
    private OutputStream out;
    private boolean compressing;
    private final int compressionThreshold;

    public SqlStreamOutput(Version version, ZoneId zoneId) throws IOException {
        this(version, zoneId, DEFAULT_COMPRESSION_THRESHOLD);
    }

    public SqlStreamOutput(Version version, ZoneId zoneId, int compressionThreshold) throws IOException {
        this.bytes = new ByteArrayOutputStream();
        this.out = bytes;
        this.compressing = false;
        this.compressionThreshold = compressionThreshold;
        Version.writeVersion(version, this);
        this.writeZoneId(zoneId);
    }

    @Override
    public void writeByte(byte b) throws IOException {
        out.write(b);
        maybeSwitchToCompression();
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        out.write(b, offset, length);
        maybeSwitchToCompression();
    }

    private void maybeSwitchToCompression() throws IOException {
        if (compressing == false && bytes.size() > compressionThreshold) {
            ByteArrayOutputStream oldBytes = bytes;
            bytes = new ByteArrayOutputStream(bytes.size());
            out = CompressorFactory.COMPRESSOR.threadLocalOutputStream(bytes);
            out.write(oldBytes.toByteArray());
            compressing = true;
        }
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        out.close();
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Should be called _after_ closing the stream - there are no guarantees otherwise.
     */
    public String streamAsString() {
        byte header = compressing ? HEADER_COMPRESSED : HEADER_UNCOMPRESSED;
        byte[] bytesWithHeader = new byte[bytes.size() + 1];
        bytesWithHeader[0] = header;
        System.arraycopy(bytes.toByteArray(), 0, bytesWithHeader, 1, bytes.size());
        return Base64.getEncoder().encodeToString(bytesWithHeader);
    }

}
