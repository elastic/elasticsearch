/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.snappy;

import io.airlift.compress.MalformedInputException;
import io.airlift.compress.snappy.SnappyHadoopStreams;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.xerial.snappy.SnappyFramedInputStream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.List;

/**
 * Snappy decompression codec for compound extensions like {@code .csv.snappy} or {@code .ndjson.snappy}.
 *
 * <p>Snappy ships with several incompatible stream framings; this codec sniffs the first byte of the
 * stream and dispatches to the matching reader:
 * <ul>
 *   <li><b>Xerial / Google framed snappy</b> (snappy-java's {@code SnappyFramedOutputStream}, the
 *   {@code snzip -t framing2} format) — recognized by the {@code \xff\x06\x00\x00sNaPpY} stream
 *   identifier. Read with snappy-java's {@link SnappyFramedInputStream} with CRC-32C verification
 *   enabled to catch silent corruption from remote blob stores.</li>
 *   <li><b>Hadoop snappy</b> (Hadoop/Hive/Spark exports, ClickHouse text exports, {@code snzip -t
 *   hadoop-snappy}) — recognized by a leading {@code 0x00} byte from the big-endian uncompressed
 *   block length. Read with aircompressor's Hadoop snappy reader via {@link SnappyHadoopStreams}.
 *   Hadoop framing carries no per-block checksum, so corruption can only be detected by snappy's
 *   own block-level validation.</li>
 * </ul>
 *
 * <p>Raw, unframed block snappy (varint length + LZ77 opcodes, as embedded inside Parquet/ORC
 * pages) is not a whole-file format and is intentionally not supported here — callers should use
 * the format reader's own page-level decompression.
 *
 * <p>The Hadoop-snappy sniff assumes the first block's uncompressed length fits in 24 bits (high
 * byte {@code 0x00}). This holds for all standard producers: Hadoop's default 256 KB block size,
 * aircompressor's {@code SnappyHadoopOutputStream} (also 256 KB), and {@code snzip}. A
 * hand-crafted single-block file with an uncompressed payload of 16 MiB or more would not be
 * detected and would fall through to the unrecognized-framing error.
 *
 * <p>Both decoders are provided by the {@code esql-datasource-compression-libs} parent plugin.
 */
public class SnappyDecompressionCodec implements DecompressionCodec {

    private static final List<String> EXTENSIONS = List.of(".snappy");

    /** First byte of the xerial/Google framed-snappy stream identifier chunk ({@code 0xff}). */
    private static final int XERIAL_MAGIC_FIRST_BYTE = 0xFF;

    /** First byte of a Hadoop-snappy stream — the high byte of a big-endian uncompressed block length. */
    private static final int HADOOP_FIRST_BYTE = 0x00;

    /** Stateless factory; reused across calls to avoid per-decompress allocation. */
    private static final SnappyHadoopStreams HADOOP_STREAMS = new SnappyHadoopStreams();

    @Override
    public String name() {
        return "snappy";
    }

    @Override
    public List<String> extensions() {
        return EXTENSIONS;
    }

    @Override
    public InputStream decompress(InputStream raw) throws IOException {
        // This method takes ownership of {@code raw}: the returned stream's close() closes the
        // pushback wrapper which closes raw. If we throw before returning that stream, we must
        // close raw ourselves or the caller (which has already lost the reference) leaks it.
        PushbackInputStream pushback = new PushbackInputStream(raw, 1);
        try {
            int first = pushback.read();
            if (first == -1) {
                // Empty stream — let the xerial reader produce its standard end-of-stream behavior.
                return new SnappyFramedInputStream(pushback, /* verifyCrc32c= */ true);
            }
            pushback.unread(first);
            return switch (first) {
                case XERIAL_MAGIC_FIRST_BYTE -> new SnappyFramedInputStream(pushback, /* verifyCrc32c= */ true);
                case HADOOP_FIRST_BYTE -> new HadoopErrorTranslatingStream(HADOOP_STREAMS.createInputStream(pushback));
                default -> throw new IOException(
                    Strings.format(
                        "unrecognized snappy stream framing (first byte 0x%02x); expected xerial framed snappy "
                            + "(magic '\\xff\\x06\\x00\\x00sNaPpY') or Hadoop snappy (big-endian int32-prefixed "
                            + "blocks). Raw block snappy is not supported as a whole-file format",
                        first
                    )
                );
            };
        } catch (Throwable t) {
            IOUtils.closeWhileHandlingException(pushback);
            throw t;
        }
    }

    /**
     * Translates aircompressor's {@link MalformedInputException} (an unchecked
     * {@link RuntimeException}) thrown during reads into {@link IOException}, matching the
     * checked contract that the codec advertises and that the xerial reader satisfies.
     * Aircompressor uses {@code RuntimeException} for compressed-stream corruption, but
     * callers in the data-source pipeline (e.g. format readers) only catch {@code IOException}
     * and would otherwise see the corruption as an unrecoverable crash.
     */
    private static final class HadoopErrorTranslatingStream extends FilterInputStream {
        HadoopErrorTranslatingStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            try {
                return in.read();
            } catch (MalformedInputException e) {
                throw translate(e);
            }
        }

        @Override
        public int read(byte[] b) throws IOException {
            // FilterInputStream.read(byte[]) delegates straight to in.read(b, 0, b.length), bypassing
            // our overridden read(byte[], int, int). Route it through this.read so the catch applies.
            return read(b, 0, b.length);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                return in.read(b, off, len);
            } catch (MalformedInputException e) {
                throw translate(e);
            }
        }

        private static IOException translate(MalformedInputException e) {
            String detail = e.getMessage();
            return new IOException(
                Strings.format(
                    "malformed Hadoop-snappy stream at offset [%d]%s",
                    e.getOffset(),
                    detail == null || detail.isEmpty() ? "" : ": " + detail
                ),
                e
            );
        }
    }
}
