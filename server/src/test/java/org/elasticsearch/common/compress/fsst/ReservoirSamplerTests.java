/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.compress.fsst;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ReservoirSamplerTests extends ESTestCase {

    public void testEmptyInput() {
        ReservoirSampler sampler = new ReservoirSampler();
        assertTrue(sampler.getSample().isEmpty());
    }

    public void testZeroLengthLinesAreIgnored() {
        ReservoirSampler sampler = new ReservoirSampler();
        sampler.processLine(new byte[0], 0, 0);
        sampler.processLine("hello".getBytes(StandardCharsets.UTF_8), 0, 0);
        assertTrue(sampler.getSample().isEmpty());
    }

    public void testSingleShortLine() {
        ReservoirSampler sampler = new ReservoirSampler();
        byte[] line = "hello world".getBytes(StandardCharsets.UTF_8);
        sampler.processLine(line, 0, line.length);

        List<byte[]> sample = sampler.getSample();
        assertEquals(1, sample.size());
        assertArrayEquals(line, sample.get(0));
    }

    public void testOffsetIsRespected() {
        ReservoirSampler sampler = new ReservoirSampler();
        byte[] data = "XXXhelloYYY".getBytes(StandardCharsets.UTF_8);
        sampler.processLine(data, 3, 5);

        List<byte[]> sample = sampler.getSample();
        assertEquals(1, sample.size());
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), sample.get(0));
    }

    public void testDeepCopyIsMade() {
        ReservoirSampler sampler = new ReservoirSampler();
        byte[] line = "hello".getBytes(StandardCharsets.UTF_8);
        sampler.processLine(line, 0, line.length);

        line[0] = 'X';
        assertNotEquals(line[0], sampler.getSample().get(0)[0]);
        assertEquals((byte) 'h', sampler.getSample().get(0)[0]);
    }

    public void testSmallInputBelowTargetIsFullyCollected() {
        ReservoirSampler sampler = new ReservoirSampler();
        int totalBytes = 0;
        for (int i = 0; i < 100; i++) {
            byte[] line = ("line-" + i).getBytes(StandardCharsets.UTF_8);
            sampler.processLine(line, 0, line.length);
            totalBytes += line.length;
        }

        int sampleBytes = sampler.getSample().stream().mapToInt(b -> b.length).sum();
        assertEquals(totalBytes, sampleBytes);
    }

    public void testSampleSizeIsBounded() {
        int target = 1024;
        ReservoirSampler sampler = new ReservoirSampler(target);
        int sampleMax = 2 * target;

        for (int i = 0; i < 10_000; i++) {
            byte[] line = ("term-" + i + "-padding-to-make-it-longer").getBytes(StandardCharsets.UTF_8);
            sampler.processLine(line, 0, line.length);
        }

        int sampleBytes = sampler.getSample().stream().mapToInt(b -> b.length).sum();
        assertTrue("sample size " + sampleBytes + " should not exceed " + sampleMax, sampleBytes <= sampleMax);
        assertFalse(sampler.getSample().isEmpty());
    }

    public void testDefaultTargetSizeIsBounded() {
        ReservoirSampler sampler = new ReservoirSampler();
        int defaultMax = 2 * FSST.FSST_SAMPLETARGET;

        for (int i = 0; i < 100_000; i++) {
            byte[] line = ("key-" + i).getBytes(StandardCharsets.UTF_8);
            sampler.processLine(line, 0, line.length);
        }

        int sampleBytes = sampler.getSample().stream().mapToInt(b -> b.length).sum();
        assertTrue("sample size " + sampleBytes + " should not exceed " + defaultMax, sampleBytes <= defaultMax);
    }

    public void testCustomTargetSize() {
        int target = 128 * 1024;
        ReservoirSampler sampler = new ReservoirSampler(target);

        for (int i = 0; i < 100_000; i++) {
            byte[] line = ("service-" + i + ".example.com").getBytes(StandardCharsets.UTF_8);
            sampler.processLine(line, 0, line.length);
        }

        int sampleBytes = sampler.getSample().stream().mapToInt(b -> b.length).sum();
        assertTrue("sample should have collected data", sampleBytes > 0);
        assertTrue("sample size " + sampleBytes + " should not exceed " + (2 * target), sampleBytes <= 2 * target);
    }

    public void testLongLineIsChunked() {
        ReservoirSampler sampler = new ReservoirSampler();

        // A line longer than FSST_SAMPLELINE (512) should be split into chunks
        byte[] longLine = new byte[1500];
        for (int i = 0; i < longLine.length; i++) {
            longLine[i] = (byte) ('a' + (i % 26));
        }
        sampler.processLine(longLine, 0, longLine.length);

        List<byte[]> sample = sampler.getSample();
        assertTrue("long line should produce multiple chunks", sample.size() >= 2);

        int totalSampled = sample.stream().mapToInt(b -> b.length).sum();
        assertEquals(longLine.length, totalSampled);
    }

    public void testReset() {
        ReservoirSampler sampler = new ReservoirSampler();
        byte[] line = "hello".getBytes(StandardCharsets.UTF_8);
        sampler.processLine(line, 0, line.length);
        assertFalse(sampler.getSample().isEmpty());

        sampler.reset();
        assertTrue(sampler.getSample().isEmpty());

        sampler.processLine(line, 0, line.length);
        assertEquals(1, sampler.getSample().size());
    }

    public void testSampleContainsValidData() {
        int target = 2048;
        ReservoirSampler sampler = new ReservoirSampler(target);

        for (int i = 0; i < 10_000; i++) {
            byte[] line = String.format("host-%05d.example.com", i).getBytes(StandardCharsets.UTF_8);
            sampler.processLine(line, 0, line.length);
        }

        for (byte[] chunk : sampler.getSample()) {
            assertTrue("chunks should not be empty", chunk.length > 0);
            assertTrue("chunks should not exceed SAMPLELINE size", chunk.length <= FSST.FSST_SAMPLELINE);
        }
    }

    public void testSampleProducesUsableSymbolTable() {
        ReservoirSampler sampler = new ReservoirSampler();

        for (int i = 0; i < 1000; i++) {
            byte[] line = String.format("metric.cpu.usage.host-%03d", i).getBytes(StandardCharsets.UTF_8);
            sampler.processLine(line, 0, line.length);
        }

        FSST.SymbolTable table = FSST.SymbolTable.buildSymbolTable(sampler.getSample());
        assertNotNull(table);
        byte[] exported = table.exportToBytes();
        assertTrue("symbol table should have non-trivial size", exported.length > 0);
    }
}
