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
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataInput;
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
public class FSSTCompressBenchmark {

    @Param("")
    public String dataset;

    private byte[] input;
    private int[] offsets;
    private byte[] outBuf;
    private int[] outOffsets;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class CompressionMetrics {
        public double compressionRatio;
    }

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
        input = concatenateTo8mb(contentBytes);
        offsets = new int[] { 0, MB_8 };
        outBuf = new byte[MB_8];
        outOffsets = new int[2];
    }

    @Benchmark
    public void compressFSST(Blackhole bh, CompressionMetrics metrics) {
        List<byte[]> sample = FSST.makeSample(input, offsets);
        var symbolTable = FSST.SymbolTable.buildSymbolTable(sample);
        symbolTable.compressBulk(1, input, offsets, outBuf, outOffsets);
        bh.consume(outBuf);
        bh.consume(outOffsets);

        int uncompressedSize = offsets[1];
        int compressedSize = outOffsets[1];
        metrics.compressionRatio = compressedSize / (double) uncompressedSize;
    }

    @Benchmark
    public void compressLZ4Fast(Blackhole bh, CompressionMetrics metrics) throws IOException {
        int inputSize = offsets[1];

        var dataInput = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(input)));
        var dataOutput = new ByteArrayDataOutput(outBuf);

        Compressor compressor = CompressionMode.FAST.newCompressor();
        compressor.compress(dataInput, dataOutput);

        long compressedSize = dataOutput.getPosition();
        bh.consume(dataOutput);

        metrics.compressionRatio = compressedSize / (double) inputSize;
    }

    // @Benchmark
    // public void compressLZ4High(Blackhole bh, CompressionMetrics metrics) throws IOException {
    // int inputSize = offsets[1];
    //
    // var dataInput = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(input)));
    // var dataOutput = new ByteArrayDataOutput(outBuf);
    //
    // Compressor compressor = CompressionMode.HIGH_COMPRESSION.newCompressor();
    // compressor.compress(dataInput, dataOutput);
    //
    // long compressedSize = dataOutput.getPosition();
    // bh.consume(dataOutput);
    //
    // metrics.compressionRatio = compressedSize / (double) inputSize;
    // }
}
