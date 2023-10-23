/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.benchmark.hash;

import org.elasticsearch.common.hash.Murmur3Hasher;
import org.elasticsearch.common.hash.MurmurHash3;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class Murmur3HasherBenchmark {

    Murmur3Hasher hasher;
    String[] hashMeIfYouCan;
    byte[][] hashBytesIfYouCan;
    MurmurHash3.Hash128 hash128;

    @Setup
    public void setUp() {
        hasher = new Murmur3Hasher(0L);
        hashMeIfYouCan = new String[]{
            "Lorem ipsum dolor sit amet,",
            "consectetur adipiscing elit,",
            "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
            "Facilisis magna etiam tempor orci eu lobortis elementum.",
            "Sapien faucibus et molestie ac feugiat sed lectus.",
            "At imperdiet dui accumsan sit.",
            "Dignissim enim sit amet venenatis urna cursus eget.",
            "Ipsum dolor sit amet consectetur.",
            "Duis ultricies lacus sed turpis.",
            "Viverra nibh cras pulvinar mattis nunc sed.",
            "Vel elit scelerisque mauris pellentesque pulvinar pellentesque habitant morbi tristique.",
            "Dolor morbi non arcu risus quis.",
            "Vitae suscipit tellus mauris a diam maecenas sed enim.",
            "Neque vitae tempus quam pellentesque nec.",
            "Velit sed ullamcorper morbi tincidunt ornare massa.",
            "In pellentesque massa placerat duis ultricies lacus sed."
        };
        hashBytesIfYouCan = new byte[hashMeIfYouCan.length][];
        for (int i = 0; i < hashMeIfYouCan.length; i++) {
            hashBytesIfYouCan[i] = hashMeIfYouCan[i].getBytes(StandardCharsets.UTF_8);
        }
        hash128 = new MurmurHash3.Hash128();
    }

    @Benchmark
    public MurmurHash3.Hash128 hashString() {
        String[] hashMeIfYouCan = this.hashMeIfYouCan;
        Murmur3Hasher hasher = this.hasher;
        hasher.reset();
        for (String s : hashMeIfYouCan) {
            hasher.update(s.getBytes(StandardCharsets.UTF_8));
        }
        return hasher.digestHash(hash128);
    }

    @Benchmark
    public MurmurHash3.Hash128 hashStringOld() {
        String[] hashMeIfYouCan = this.hashMeIfYouCan;
        Murmur3Hasher hasher = this.hasher;
        hasher.reset();
        for (String s : hashMeIfYouCan) {
            hasher.updateOld(s.getBytes(StandardCharsets.UTF_8));
        }
        return hasher.digestHash(hash128);
    }
    @Benchmark
    public byte[] hashStringOldToByteArray() {
        String[] hashMeIfYouCan = this.hashMeIfYouCan;
        Murmur3Hasher hasher = this.hasher;
        hasher.reset();
        for (String s : hashMeIfYouCan) {
            hasher.updateOld(s.getBytes(StandardCharsets.UTF_8));
        }
        return hasher.digestHash(hash128).getBytes();
    }

    @Benchmark
    public MurmurHash3.Hash128 hashBytes() {
        byte[][] hashBytesIfYouCan = this.hashBytesIfYouCan;
        Murmur3Hasher hasher = this.hasher;
        hasher.reset();
        for (byte[] b : hashBytesIfYouCan) {
            hasher.update(b);
        }
        return hasher.digestHash(hash128);
    }

    @Benchmark
    public MurmurHash3.Hash128 hashBytesOld() {
        byte[][] hashBytesIfYouCan = this.hashBytesIfYouCan;
        Murmur3Hasher hasher = this.hasher;
        hasher.reset();
        for (byte[] b : hashBytesIfYouCan) {
            hasher.updateOld(b);
        }
        return hasher.digestHash(hash128);
    }

    @Benchmark
    public byte[] hashBytesOldToByteArray() {
        byte[][] hashBytesIfYouCan = this.hashBytesIfYouCan;
        Murmur3Hasher hasher = this.hasher;
        hasher.reset();
        for (byte[] b : hashBytesIfYouCan) {
            hasher.updateOld(b);
        }
        return hasher.digestHash(hash128).getBytes();
    }

    public static void main(String[] args) throws RunnerException {
        new Runner(
            new OptionsBuilder().include(Murmur3HasherBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .addProfiler(GCProfiler.class)
                .build()
        ).run();
    }
}
