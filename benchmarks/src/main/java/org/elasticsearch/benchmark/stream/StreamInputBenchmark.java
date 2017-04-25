/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.benchmark.stream;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Fork(10)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") //invoked by benchmarking framework
public class StreamInputBenchmark {

    // 4, 16, 64, 256, 1024, 16348, 1 MB, 10 MB, 100 MB
    @Param({"4", "16", "64", "256", "1024", "16348", "1048576", "10485760", "104857600"})
    public String size;
    byte[] data;
    int numInts;

    @Setup
    public void generateData() throws IOException {
        int numBytes = Integer.valueOf(size);
        numInts = Double.valueOf(Math.ceil(((double) numBytes) / 5.0d)).intValue();
        // force worst case where there are always 5 bytes since it is a negative number
        int start = Integer.MIN_VALUE;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            for (int i = 0; i < numInts; i++) {
                out.writeVInt(start++);
            }
            data = out.bytes().toBytesRef().bytes;
        }
    }

    @Benchmark
    public int measureReadAndSumVInt() throws IOException {
        int value = 0;
        try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(data))) {
            for (int i = 0; i < numInts; i++) {
                value += input.readVInt();
            }
        }
        return value;
    }

    @Benchmark
    public int[] measureReadToArray() throws IOException {
        int[] array = new int[numInts];
        try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(data))) {
            for (int i = 0; i < numInts; i++) {
                array[i] = input.readVInt();
            }
        }
        return array;
    }
}
