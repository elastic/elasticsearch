/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Shared suppliers for {@link DataType#FLATTENED} test cases,
 * used by both {@link TestCaseSupplier} and {@link MultiRowTestCaseSupplier}.
 */
public class FlattenedCases {
    static final Supplier<BytesRef> EMPTY = json(b -> {});
    static final Supplier<BytesRef> SINGLE_KEY = json(b -> b.field(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20)));
    static final Supplier<BytesRef> MULTI_KEY = json(b -> {
        Set<String> used = new HashSet<>();
        int keys = randomIntBetween(2, 10);
        for (int i = 0; i < keys; i++) {
            b.field(uniqueAlpha(used, 1, 20), randomAlphaOfLengthBetween(1, 20));
        }
    });
    static final Supplier<BytesRef> OBJECT = json(b -> {
        Set<String> outerKeys = new HashSet<>();
        int keys = randomIntBetween(1, 5);
        for (int i = 0; i < keys; i++) {
            b.startObject(uniqueAlpha(outerKeys, 1, 10));
            Set<String> innerKeys = new HashSet<>();
            int objectKeys = randomIntBetween(1, 5);
            for (int j = 0; j < objectKeys; j++) {
                b.field(uniqueAlpha(innerKeys, 1, 10), randomAlphaOfLengthBetween(1, 10));
            }
            b.endObject();
        }
    });
    public static final Supplier<BytesRef> RANDOM = () -> randomFrom(EMPTY, SINGLE_KEY, MULTI_KEY, OBJECT).get();

    private static Supplier<BytesRef> json(CheckedConsumer<XContentBuilder, IOException> body) {
        return () -> {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                body.accept(builder);
                return new BytesRef(Strings.toString(builder.endObject()));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    /**
     * Returns a random alpha key not present in {@code used} and records it. Real flattened
     * fields collapse duplicate top-level keys at storage time, so test JSON must not contain
     * them either; otherwise the strict-duplicates parser used by {@code field_extract} would
     * reject the input. With small key lengths and a 1-5 key bucket, the chance of repeated
     * collisions in this loop is negligible.
     */
    private static String uniqueAlpha(Set<String> used, int minLength, int maxLength) {
        for (;;) {
            String candidate = randomAlphaOfLengthBetween(minLength, maxLength);
            if (used.add(candidate)) {
                return candidate;
            }
        }
    }
}
