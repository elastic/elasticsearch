/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.index.analysis.AsciiStandardAnalyzer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares Lucene's {@link StandardAnalyzer} with {@link AsciiStandardAnalyzer} on synthetic but log-shaped messages. Every value is
 * analyzed once per invocation and the benchmark touches each term, which is the minimum a consumer such as the indexing chain does.
 * The costs that a real consumer adds on top (hashing the term, writing postings) are the same for both analyzers.
 */
@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
public class StandardAnalyzerBenchmark {

    @Param({ "lucene", "ascii" })
    public String analyzer;

    /** Share of messages that contain a non-ASCII char, in percent. These use the fallback in the ascii analyzer. */
    @Param({ "0", "2", "20" })
    public int nonAsciiPercent;

    private static final int MESSAGES = 4096;

    private Analyzer impl;
    private String[] messages;

    @Setup
    public void setUp() {
        impl = switch (analyzer) {
            case "lucene" -> new StandardAnalyzer(CharArraySet.EMPTY_SET);
            case "ascii" -> new AsciiStandardAnalyzer();
            default -> throw new IllegalArgumentException("unknown analyzer [" + analyzer + "]");
        };
        Random random = new Random(42);
        messages = new String[MESSAGES];
        for (int i = 0; i < MESSAGES; i++) {
            String message = logMessage(random);
            if (random.nextInt(100) < nonAsciiPercent) {
                int at = random.nextInt(message.length());
                message = message.substring(0, at) + "café 中文" + message.substring(at);
            }
            messages[i] = message;
        }
    }

    private static String ip(Random r) {
        return r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
    }

    private static String logMessage(Random r) {
        return switch (r.nextInt(4)) {
            case 0 -> ip(r)
                + " - - [12/Nov/2026:18:13:"
                + (10 + r.nextInt(50))
                + " +0000] \"GET /api/v"
                + (1 + r.nextInt(3))
                + "/users/"
                + r.nextInt(100_000)
                + "?sort=name&limit="
                + r.nextInt(100)
                + " HTTP/1.1\" "
                + (r.nextInt(10) == 0 ? 500 : 200)
                + " "
                + r.nextInt(50_000)
                + " \"-\" \"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
                + (100 + r.nextInt(30))
                + ".0.0.0 Safari/537.36\"";
            case 1 -> "2026-11-12T18:13:"
                + (10 + r.nextInt(50))
                + ".123Z INFO [pool-"
                + r.nextInt(16)
                + "-thread-"
                + r.nextInt(8)
                + "] org.example.service.OrderProcessor - Processed order_id="
                + r.nextInt(1_000_000)
                + " customer="
                + Integer.toHexString(r.nextInt())
                + " items="
                + r.nextInt(20)
                + " total=$"
                + r.nextInt(1000)
                + "."
                + r.nextInt(100)
                + " in "
                + r.nextInt(900)
                + "ms";
            case 2 -> "Nov 12 18:13:"
                + (10 + r.nextInt(50))
                + " host-"
                + r.nextInt(100)
                + " sshd["
                + r.nextInt(30_000)
                + "]: Failed password for invalid user admin from "
                + ip(r)
                + " port "
                + r.nextInt(65_535)
                + " ssh2";
            default -> "java.lang.IllegalStateException: shard [logs-app-2026.11.12][3] is not started, state [INITIALIZING]\n"
                + "\tat org.elasticsearch.index.shard.IndexShard.verifyNotClosed(IndexShard.java:"
                + r.nextInt(5000)
                + ")\n\tat org.elasticsearch.action.bulk.TransportShardBulkAction.performOnPrimary(TransportShardBulkAction.java:"
                + r.nextInt(500)
                + ")";
        };
    }

    @Benchmark
    public void analyze(Blackhole bh) throws IOException {
        int checksum = 0;
        for (String message : messages) {
            try (TokenStream ts = impl.tokenStream("message", message)) {
                CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
                ts.reset();
                while (ts.incrementToken()) {
                    checksum += term.length() + term.buffer()[0];
                }
                ts.end();
            }
        }
        bh.consume(checksum);
    }
}
