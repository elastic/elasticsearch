/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
// first iteration is complete garbage, so make sure we really warmup
@Warmup(iterations = 3, time = 1)
// real iterations. not useful to spend tons of time here, better to fork more
@Measurement(iterations = 5, time = 1)
// engage some noise reduction
@Fork(value = 1)
public class Base64VectorBenchmark {

    @Param({ "384", "782", "1024", "1536" })
    public int dims;

    final int numVectors = 1000;

    final byte[][] bytesFloat = new byte[numVectors][];
    final byte[][] bytesBase64 = new byte[numVectors][];

    @Setup
    public void setup() throws IOException {
        for (int i = 0; i < numVectors; i++) {
            float[] vector = new float[dims];
            for (int j = 0; j < dims; j++) {
                vector[j] = ThreadLocalRandom.current().nextFloat();
            }
            // builds float version
            {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder.array("vector", vector);
                builder.endObject();
                builder.close();
                bytesFloat[i] = ((ByteArrayOutputStream) builder.getOutputStream()).toByteArray();
            }
            // build base64 version
            {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                final ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES * dims).order(ByteOrder.BIG_ENDIAN);
                buffer.asFloatBuffer().put(vector);
                builder.field("vector", Base64.getEncoder().encodeToString(buffer.array()));
                builder.endObject();
                builder.close();
                bytesBase64[i] = ((ByteArrayOutputStream) builder.getOutputStream()).toByteArray();
            }
        }
    }

    public interface FloatArrayConsumer {
        void consume(float[] value);
    }

    @Benchmark
    public void parserVectorFloats(Blackhole bh) throws IOException {
        parserVectorFloatsImpl(bh::consume);
    }

    void parserVectorFloatsImpl(FloatArrayConsumer bh) throws IOException {
        float[] vector = new float[dims];
        for (int j = 0; j < numVectors; j++) {
            int i = 0;
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytesFloat[j])) {
                parser.nextToken(); // start object
                parser.nextToken(); // field name
                parser.nextToken(); // start array
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    vector[i++] = parser.floatValue();
                }
                bh.consume(vector);
            }
        }
    }

    @Benchmark
    public void parserVectorBase64(Blackhole bh) throws IOException {
        parserVectorBase64Impl(bh::consume);
    }

    void parserVectorBase64Impl(FloatArrayConsumer bh) throws IOException {
        float[] vector = new float[dims];
        for (int j = 0; j < numVectors; j++) {
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytesBase64[j])) {
                parser.nextToken(); // start object
                parser.nextToken(); // field name
                parser.nextToken(); // value
                ByteBuffer byteBuffer = ByteBuffer.wrap(Base64.getDecoder().decode(parser.text())).order(ByteOrder.BIG_ENDIAN);
                byteBuffer.asFloatBuffer().get(vector);
                bh.consume(vector);
            }
        }
    }

    @Benchmark
    public void parserVectorBase64NoCopy(Blackhole bh) throws IOException {
        parserVectorBase64NoCopyImpl(bh::consume);
    }

    void parserVectorBase64NoCopyImpl(FloatArrayConsumer bh) throws IOException {
        float[] vector = new float[dims];
        for (int j = 0; j < numVectors; j++) {
            try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytesBase64[j])) {
                parser.nextToken(); // start object
                parser.nextToken(); // field name
                parser.nextToken(); // value

                XContentString.UTF8Bytes utfBytes = parser.optimizedText().bytes();
                ByteBuffer srcBuffer = ByteBuffer.wrap(utfBytes.bytes(), utfBytes.offset(), utfBytes.length());
                ByteBuffer decodedBuffer = Base64.getDecoder().decode(srcBuffer);
                decodedBuffer.order(ByteOrder.BIG_ENDIAN).asFloatBuffer().get(vector);
                bh.consume(vector);
            }
        }
    }
}
