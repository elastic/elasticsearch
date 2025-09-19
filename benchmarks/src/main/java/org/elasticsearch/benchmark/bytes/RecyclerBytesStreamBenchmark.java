/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.bytes;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
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

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Warmup(iterations = 3)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Fork(value = 1)
public class RecyclerBytesStreamBenchmark {

    private final AtomicReference<BytesRef> bytesRef = new AtomicReference<>(new BytesRef(16384));
    private RecyclerBytesStreamOutput streamOutput;
    private String shortString;
    private String longString;
    private String nonAsciiString;
    private String veryLongString;
    private byte[] bytes1;
    private byte[] bytes2;
    private byte[] bytes3;
    private byte[] bytes4;

    @Setup
    public void initResults() throws IOException {
        streamOutput = new RecyclerBytesStreamOutput(new BenchmarkRecycler(bytesRef));
        shortString = "K7mP9xQ2nV8wR4jL6tE3";
        longString = "xK9mP4nV7wR2jL6tE3qA8zS5yU1oI0bG9cF7hN3kM6pX2vB4lW8rT5eY9uQ1iO7aD3sZ6fH2gJ4nK8mL0xC5vB9wE7qP3tY6rU2";
        nonAsciiString =
            "k7mP9xQ2nV8wR4jL6tE3qA8zS5yU1oI0bG9cF7hN3kM6pX2vB4lW8rT5eY9uQ1iO7aD3sZ6fH2gJ4nK8mL0xC5vB9wE7qP3tY6rU2oI4zA1sF8hG5nM7kX0bV3"
                + "cL9pT6eW4yQ2uR8jN1oA5dS7fH9gK3mP6xC0vB2lñéüøåæçþðßÿì8tE4qY7rU5iZ9aO1sF4hG6nM3kX8pV0bL2cW9eT7yQ5uR1jN4oA8dS6fH3gK0mP9xC"
                + "7vB5lW2tE8qY4rU1iZ6aO3sF9hG0nM7kX5pV8bL4cW2eTíîïôõöúûüÑÉÜ6yQ9uR3jN1oA0dS8fH5g";
        veryLongString =
            "xK9mP4nV7wR2jL6tE3qA8zS5yU1oI0bG9cF7hN3kM6pX2vB4lW8rT5eY9uQ1iO7aD3sZ6fH2gJ4nK8mL0xC5vB9wE7qP3tY6rU2oI4zA1sF8hG5nM7kX0bV3cL9p"
                + "T6eW4yQ2uR8jN1oA5dS7fH9gK3mP6xC0vB2lW8tE4qY7rU5iZ9aO1sF4hG6nM3kX8pV0bL2cW9eT7yQ5uR1jN4oA8dS6fH3gK0mP9xC7vB5lW2tE8qY4rU1i"
                + "Z6aO3sF9hG0nM7kX5pV8bL4cW2eT6yQ9uR3jN1oA0dS8fH5gK2mP7xC4vB9lW1tE5qY8rU0iZ3aO6sF2hG9nM4kX7pV1bL5cW8eT0yQ6uR2jN9oA3dS1fH4g"
                + "K7mP0xC8vB6lW3tE9qY2rU5iZ8aO0sF3hG1nM9kX2pV6bL0cW4eT7yQ1uR5jN8oA6dS9fH2gK4mP1xC0vB8lW6tE2qY5rU9iZ1aO4sF7hG3nM0kX6pV2bL8"
                + "cW5eT9yQ3uR7jN0oA4dS2fH8gK1mP5xC9vB3lW0tE6qY9rU2iZ5aO8sF1hG4nM7kX3pV0bL6cW9eT2yQ5u";
        ThreadLocalRandom random = ThreadLocalRandom.current();
        bytes1 = new byte[327];
        bytes2 = new byte[712];
        bytes3 = new byte[1678];
        bytes4 = new byte[16387 * 4];
        random.nextBytes(bytes1);
        random.nextBytes(bytes2);
        random.nextBytes(bytes3);
        random.nextBytes(bytes4);
    }

    @Benchmark
    public void writeByte() throws IOException {
        streamOutput.seek(1);
        for (byte item : bytes1) {
            streamOutput.writeByte(item);
        }
        for (byte item : bytes2) {
            streamOutput.writeByte(item);
        }
        for (byte item : bytes3) {
            streamOutput.writeByte(item);
        }
    }

    @Benchmark
    public void writeBytes() throws IOException {
        streamOutput.seek(1);
        streamOutput.writeBytes(bytes1, 0, bytes1.length);
        streamOutput.writeBytes(bytes2, 0, bytes2.length);
        streamOutput.writeBytes(bytes3, 0, bytes3.length);
    }

    @Benchmark
    public void writeBytesAcrossPageBoundary() throws IOException {
        streamOutput.seek(16384 - 1000);
        streamOutput.writeBytes(bytes1, 0, bytes1.length);
        streamOutput.writeBytes(bytes2, 0, bytes2.length);
        streamOutput.writeBytes(bytes3, 0, bytes3.length);
    }

    @Benchmark
    public void writeBytesMultiPage() throws IOException {
        streamOutput.seek(16384 - 1000);
        streamOutput.writeBytes(bytes4, 0, bytes4.length);
    }

    @Benchmark
    public void writeString() throws IOException {
        streamOutput.seek(1);
        streamOutput.writeString(shortString);
        streamOutput.writeString(longString);
        streamOutput.writeString(nonAsciiString);
        streamOutput.writeString(veryLongString);
    }

    private record BenchmarkRecycler(AtomicReference<BytesRef> bytesRef) implements Recycler<BytesRef> {

        @Override
        public V<BytesRef> obtain() {
            BytesRef recycledBytesRef = bytesRef.getAndSet(null);
            final BytesRef localBytesRef;
            final boolean recycled;
            if (recycledBytesRef != null) {
                recycled = true;
                localBytesRef = recycledBytesRef;
            } else {
                recycled = false;
                localBytesRef = new BytesRef(16384);
            }
            return new V<>() {
                @Override
                public BytesRef v() {
                    return localBytesRef;
                }

                @Override
                public boolean isRecycled() {
                    return recycled;
                }

                @Override
                public void close() {
                    if (recycled) {
                        bytesRef.set(localBytesRef);
                    }
                }
            };
        }

        @Override
        public int pageSize() {
            return 16384;
        }
    }
}
