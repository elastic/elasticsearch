/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.compress;

import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.compress.fsst.FSST;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class FSSTDecompressBenchmark {

    // @Param({ "fsst", "lz4_high", "lz4_fast" })
    @Param({ "fsst", "lz4_fast" })
    public String compressionType;

    @Param("")
    public String dataset;

    // original file
    private int originalSize;
    private byte[] input;
    private int[] offsets;

    // compressed
    private byte[] outBuf;
    private int[] outOffsets;
    private int compressedSize;

    // decompressed
    private byte[] decompressBuf;

    // fsst specific
    private FSST.SymbolTable symbolTable;

    private static final int MB_8 = 8 * 1024 * 1024;

    private byte[] concatenateTo8mb(byte[] contentBytes) {
        byte[] bytes = new byte[MB_8 + 8];
        int i = 0;
        while (i < MB_8) {
            int remaining = MB_8 - i;
            int len = Math.min(contentBytes.length, remaining);
            System.arraycopy(contentBytes, 0, bytes, i, len);
            i += len;
        }
        return bytes;
    }

    @Setup(Level.Trial)
    public void setup() throws IOException {
        String content = Files.readString(Path.of(dataset), StandardCharsets.UTF_8);
        byte[] contentBytes = FSST.toBytes(content);
        originalSize = MB_8;
        input = concatenateTo8mb(contentBytes);
        offsets = new int[] { 0, originalSize };

        outBuf = new byte[input.length];
        outOffsets = new int[2];

        decompressBuf = new byte[input.length];

        if (compressionType.equals("fsst")) {
            List<byte[]> sample = FSST.makeSample(input, offsets);
            symbolTable = FSST.SymbolTable.buildSymbolTable(sample);
            symbolTable.compressBulk(1, input, offsets, outBuf, outOffsets);
            compressedSize = outOffsets[1];
        } else if (compressionType.equals("lz4_fast")) {
            var dataInput = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(input, 0, originalSize)));
            var dataOutput = new ByteArrayDataOutput(outBuf);
            Compressor compressor = CompressionMode.FAST.newCompressor();
            compressor.compress(dataInput, dataOutput);
            compressedSize = dataOutput.getPosition();
        } else if (compressionType.equals("lz4_high")) {
            var dataInput = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(input, 0, originalSize)));
            var dataOutput = new ByteArrayDataOutput(outBuf);
            Compressor compressor = CompressionMode.HIGH_COMPRESSION.newCompressor();
            compressor.compress(dataInput, dataOutput);
            compressedSize = dataOutput.getPosition();
        }
    }

    @Benchmark
    public void decompress(Blackhole bh) throws IOException {
        if (compressionType.equals("fsst")) {
            byte[] symbolTableBytes = symbolTable.exportToBytes();
            FSST.Decoder decoder = FSST.Decoder.readFrom(symbolTableBytes);
            int decompressedLen = FSST.decompress(outBuf, 0, outOffsets[1], decoder, decompressBuf);
            // assert Arrays.equals(input, 0, originalSize, decompressBuf, 0, originalSize);
            bh.consume(decompressBuf);
            bh.consume(decompressedLen);
        } else if (compressionType.equals("lz4_fast")) {
            Decompressor decompressor = CompressionMode.FAST.newDecompressor();
            var dataInput = new ByteArrayDataInput(outBuf, 0, compressedSize);
            var outBytesRef = new BytesRef(decompressBuf);
            decompressor.decompress(dataInput, originalSize, 0, originalSize, outBytesRef);
            // assert Arrays.equals(input, 0, originalSize, outBytesRef.bytes, 0, originalSize);
            bh.consume(outBytesRef);
        } else if (compressionType.equals("lz4_high")) {
            Decompressor decompressor = CompressionMode.HIGH_COMPRESSION.newDecompressor();
            var dataInput = new ByteArrayDataInput(outBuf, 0, compressedSize);
            var outBytesRef = new BytesRef(decompressBuf);
            decompressor.decompress(dataInput, originalSize, 0, originalSize, outBytesRef);
            // assert Arrays.equals(input, 0, originalSize, outBytesRef.bytes, 0, originalSize);
            bh.consume(outBytesRef);
        }
    }
}
