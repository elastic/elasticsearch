/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.benchmark.xcontent;

import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1)
public class KnnQueryVectorParseBenchmark {

    @Param({ "256", "768", "1536" })
    int dims;

    private byte[] floatEncodedQuery;
    private byte[] base64EncodedQuery;
    private byte[] knnSearchFloatQuery;
    private byte[] knnSearchBase64Query;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        float[] vector = new float[dims];
        for (int i = 0; i < dims; i++) {
            vector[i] = ThreadLocalRandom.current().nextFloat();
        }

        // base64 encoded string (needed for both query types)
        byte[] bytes = new byte[Float.BYTES * dims];
        ByteBuffer.wrap(bytes).asFloatBuffer().put(vector);
        String encoded = Base64.getEncoder().encodeToString(bytes);

        // JSON array encoded query_vector (KnnVectorQueryBuilder)
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject(KnnVectorQueryBuilder.NAME);
            builder.field(KnnVectorQueryBuilder.FIELD_FIELD.getPreferredName(), "vector");
            builder.field(KnnVectorQueryBuilder.K_FIELD.getPreferredName(), 10);
            builder.field(KnnVectorQueryBuilder.NUM_CANDS_FIELD.getPreferredName(), 50);
            builder.field(KnnVectorQueryBuilder.QUERY_VECTOR_FIELD.getPreferredName(), vector);
            builder.endObject();
            builder.endObject();
            builder.flush();
            floatEncodedQuery = ((ByteArrayOutputStream) builder.getOutputStream()).toByteArray();
        }

        // base64 encoded query_vector_base64 (KnnVectorQueryBuilder)
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject(KnnVectorQueryBuilder.NAME);
            builder.field(KnnVectorQueryBuilder.FIELD_FIELD.getPreferredName(), "vector");
            builder.field(KnnVectorQueryBuilder.K_FIELD.getPreferredName(), 10);
            builder.field(KnnVectorQueryBuilder.NUM_CANDS_FIELD.getPreferredName(), 50);
            builder.field(KnnVectorQueryBuilder.QUERY_VECTOR_BASE64_FIELD.getPreferredName(), encoded);
            builder.endObject();
            builder.endObject();
            builder.flush();
            base64EncodedQuery = ((ByteArrayOutputStream) builder.getOutputStream()).toByteArray();
        }

        // JSON array encoded query_vector (KnnSearchBuilder)
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(KnnSearchBuilder.FIELD_FIELD.getPreferredName(), "vector");
            builder.field(KnnSearchBuilder.K_FIELD.getPreferredName(), 10);
            builder.field(KnnSearchBuilder.NUM_CANDS_FIELD.getPreferredName(), 50);
            builder.field(KnnSearchBuilder.QUERY_VECTOR_FIELD.getPreferredName(), vector);
            builder.endObject();
            builder.flush();
            knnSearchFloatQuery = ((ByteArrayOutputStream) builder.getOutputStream()).toByteArray();
        }

        // base64 encoded query_vector_base64 (KnnSearchBuilder)
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field(KnnSearchBuilder.FIELD_FIELD.getPreferredName(), "vector");
            builder.field(KnnSearchBuilder.K_FIELD.getPreferredName(), 10);
            builder.field(KnnSearchBuilder.NUM_CANDS_FIELD.getPreferredName(), 50);
            builder.field(KnnSearchBuilder.QUERY_VECTOR_BASE64_FIELD.getPreferredName(), encoded);
            builder.endObject();
            builder.flush();
            knnSearchBase64Query = ((ByteArrayOutputStream) builder.getOutputStream()).toByteArray();
        }
    }

    @Benchmark
    public void parseQueryVectorArray(Blackhole bh) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, floatEncodedQuery)) {
            parser.nextToken(); // start root object
            parser.nextToken(); // field name knn
            parser.nextToken(); // start knn object
            KnnVectorQueryBuilder builder = KnnVectorQueryBuilder.fromXContent(parser);
            bh.consume(builder);
        }
    }

    @Benchmark
    public void parseQueryVectorBase64(Blackhole bh) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, base64EncodedQuery)) {
            parser.nextToken();
            parser.nextToken();
            parser.nextToken();
            KnnVectorQueryBuilder builder = KnnVectorQueryBuilder.fromXContent(parser);
            bh.consume(builder);
        }
    }

    @Benchmark
    public void parseKnnSearchArray(Blackhole bh) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, knnSearchFloatQuery)) {
            parser.nextToken();
            KnnSearchBuilder.Builder builder = KnnSearchBuilder.fromXContent(parser);
            bh.consume(builder);
        }
    }

    @Benchmark
    public void parseKnnSearchBase64(Blackhole bh) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, knnSearchBase64Query)) {
            parser.nextToken();
            KnnSearchBuilder.Builder builder = KnnSearchBuilder.fromXContent(parser);
            bh.consume(builder);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        // Cleanup if needed
    }
}
