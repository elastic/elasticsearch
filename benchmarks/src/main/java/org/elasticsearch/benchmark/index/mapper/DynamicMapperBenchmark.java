/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
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

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Fork(value = 3)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class DynamicMapperBenchmark {

    @Param({ "1600172297" })
    private long seed;

    private Random random;
    private SourceToParse[] sources;

    @Setup
    public void setUp() {
        this.random = new Random(seed);
        this.sources = generateRandomDocuments(500);
    }

    private SourceToParse[] generateRandomDocuments(int count) {
        var docs = new SourceToParse[count];
        for (int i = 0; i < count; i++) {
            docs[i] = generateRandomDocument();
        }
        return docs;
    }

    private SourceToParse generateRandomDocument() {
        int textFields = 50;
        int intFields = 50;
        int floatFields = 50;
        int objFields = 10;
        int objFieldDepth = 10;
        int fieldValueCountMax = 25;
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        for (int i = 0; i < textFields; i++) {
            if (random.nextBoolean()) {
                StringBuilder fieldValueBuilder = generateTextField(fieldValueCountMax);
                builder.append("\"text_field_").append(i).append("\":").append(fieldValueBuilder).append(",");
            }
        }
        for (int i = 0; i < intFields; i++) {
            if (random.nextBoolean()) {
                int fieldValueCount = random.nextInt(fieldValueCountMax);
                builder.append("\"int_field_")
                    .append(i)
                    .append("\":")
                    .append(Arrays.toString(IntStream.generate(() -> random.nextInt()).limit(fieldValueCount).toArray()))
                    .append(",");
            }
        }
        for (int i = 0; i < floatFields; i++) {
            if (random.nextBoolean()) {
                int fieldValueCount = random.nextInt(fieldValueCountMax);
                builder.append("\"float_field_")
                    .append(i)
                    .append("\":")
                    .append(Arrays.toString(DoubleStream.generate(() -> random.nextFloat()).limit(fieldValueCount).toArray()))
                    .append(",");
            }
        }
        for (int i = 0; i < objFields; i++) {
            final int idx = i;
            if (random.nextBoolean()) {
                continue;
            }
            int objFieldDepthActual = random.nextInt(1, objFieldDepth);
            String objFieldPrefix = Stream.generate(() -> "obj_field_" + idx).limit(objFieldDepthActual).collect(Collectors.joining("."));
            for (int j = 0; j < textFields; j++) {
                if (random.nextBoolean()) {
                    StringBuilder fieldValueBuilder = generateTextField(fieldValueCountMax);
                    builder.append("\"")
                        .append(objFieldPrefix)
                        .append(".text_field_")
                        .append(j)
                        .append("\":")
                        .append(fieldValueBuilder)
                        .append(",");
                }
            }
            for (int j = 0; j < intFields; j++) {
                if (random.nextBoolean()) {
                    int fieldValueCount = random.nextInt(fieldValueCountMax);
                    builder.append("\"")
                        .append(objFieldPrefix)
                        .append(".int_field_")
                        .append(j)
                        .append("\":")
                        .append(Arrays.toString(IntStream.generate(() -> random.nextInt()).limit(fieldValueCount).toArray()))
                        .append(",");
                }
            }
            for (int j = 0; j < floatFields; j++) {
                if (random.nextBoolean()) {
                    int fieldValueCount = random.nextInt(fieldValueCountMax);
                    builder.append("\"")
                        .append(objFieldPrefix)
                        .append(".float_field_")
                        .append(j)
                        .append("\":")
                        .append(Arrays.toString(DoubleStream.generate(() -> random.nextFloat()).limit(fieldValueCount).toArray()))
                        .append(",");
                }
            }
        }
        if (builder.charAt(builder.length() - 1) == ',') {
            builder.deleteCharAt(builder.length() - 1);
        }
        builder.append("}");
        return new SourceToParse(UUIDs.randomBase64UUID(), new BytesArray(builder.toString()), XContentType.JSON);
    }

    private StringBuilder generateTextField(int fieldValueCountMax) {
        int fieldValueCount = random.nextInt(fieldValueCountMax);
        StringBuilder fieldValueBuilder = new StringBuilder();
        fieldValueBuilder.append("[");
        for (int j = 0; j < fieldValueCount - 1; j++) {
            fieldValueBuilder.append("\"").append(randomString(6)).append("\"").append(",");
        }
        return fieldValueBuilder.append("\"").append(randomString(6)).append("\"").append("]");
    }

    private String randomString(int maxLength) {
        var length = random.nextInt(maxLength);
        var builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((byte) (32 + random.nextInt(94)));
        }
        return builder.toString();
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private <T> T randomFrom(T... items) {
        return items[random.nextInt(items.length)];
    }

    @Benchmark
    public List<LuceneDocument> benchmarkDynamicallyCreatedFields() throws Exception {
        MapperService mapperService = MapperServiceFactory.create("{}");
        for (int i = 0; i < 25; i++) {
            DocumentMapper documentMapper = mapperService.documentMapper();
            Mapping mapping = null;
            if (documentMapper == null) {
                documentMapper = DocumentMapper.createEmpty(mapperService);
                mapping = documentMapper.mapping();
            }
            ParsedDocument doc = documentMapper.parse(randomFrom(sources));
            if (mapping != null) {
                doc.addDynamicMappingsUpdate(mapping);
            }
            if (doc.dynamicMappingsUpdate() != null) {
                mapperService.merge(
                    "_doc",
                    new CompressedXContent(XContentHelper.toXContent(doc.dynamicMappingsUpdate(), XContentType.JSON, false)),
                    MapperService.MergeReason.MAPPING_UPDATE
                );
            }
        }
        return mapperService.documentMapper().parse(randomFrom(sources)).docs();
    }
}
