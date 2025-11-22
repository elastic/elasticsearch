/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.common.settings;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Fork(1)
@Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 5, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class SecureStringCacheBenchmark {

    @Param({ "SECURE_STRING_CACHE", "STRING_CACHE", "LOCKING_CACHE", "SPINNING_CACHE" })
    private String typeParam;

    @Param({ "128" })
    private int stringLength;

    private Cache cache;
    private SecureString secureString;

    enum Type {
        SECURE_STRING_CACHE(new SecureStringCache()),
        STRING_CACHE(new StringCache()),
        LOCKING_CACHE(new LockingStringCache()),
        SPINNING_CACHE(new SpinningCache());

        final Cache cache;

        Type(Cache cache) {
            this.cache = cache;
        }
    }

    @Setup
    public void setup() {
        cache = Type.valueOf(typeParam).cache;
        char[] chars = new char[stringLength];
        Arrays.fill(chars, 'a');
        secureString = new SecureString(chars);
    }

    @Group
    @GroupThreads(1)
    @Benchmark
    public void writer() {
        cache.set(secureString);
    }

    @Group
    @GroupThreads(10)
    @Benchmark
    public void reader(Blackhole blackhole) {
        blackhole.consume(cache.get());
    }

    interface Cache {
        void set(SecureString value);

        SecureString get();
    }

    static class StringCache implements Cache {
        private volatile String value = "";

        @Override
        public void set(SecureString value) {
            this.value = value.toString();
        }

        @Override
        public SecureString get() {
            return new SecureString(value.toCharArray());
        }
    }

    static class SecureStringCache implements Cache {
        private volatile SecureString value = new SecureString(new char[0]);

        @Override
        public void set(SecureString value) {
            this.value = value.clone();
        }

        @Override
        public SecureString get() {
            return value.clone();
        }
    }

    static class LockingStringCache implements Cache {
        private char[] value = new char[0];
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private final ReleasableLock readLock = new ReleasableLock(lock.readLock());
        private final ReleasableLock writeLock = new ReleasableLock(lock.writeLock());

        @Override
        public void set(SecureString value) {
            try (var ignored = writeLock.acquire()) {
                char[] old = this.value;
                char[] chars = value.getChars();
                this.value = Arrays.copyOf(chars, chars.length);
                Arrays.fill(old, '\0');
            }
        }

        @Override
        public SecureString get() {
            try (var ignored = readLock.acquire()) {
                return new SecureString(value);
            }
        }
    }

    static class SpinningCache implements Cache {
        private volatile char[] value = new char[0];

        @Override
        public void set(SecureString value) {
            char[] old = this.value;
            char[] chars = value.getChars();
            this.value = Arrays.copyOf(chars, chars.length);
            Arrays.fill(old, '\0');
        }

        @Override
        public SecureString get() {
            while (true) {
                char[] chars = this.value;
                char[] copy = Arrays.copyOf(chars, chars.length);
                if (chars == this.value) {
                    return new SecureString(copy);
                }
            }
        }
    }

}
