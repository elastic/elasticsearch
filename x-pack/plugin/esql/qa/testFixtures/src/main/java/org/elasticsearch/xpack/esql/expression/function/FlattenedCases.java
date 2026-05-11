/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

/**
 * Shared suppliers for {@link DataType#FLATTENED} test cases,
 * used by both {@link TestCaseSupplier} and {@link MultiRowTestCaseSupplier}.
 */
class FlattenedCases {
    static final Supplier<BytesRef> EMPTY = json(b -> {});
    static final Supplier<BytesRef> SINGLE_KEY = json(b -> b.field(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20)));
    static final Supplier<BytesRef> MULTI_KEY = json(b -> {
        int keys = randomIntBetween(2, 10);
        for (int i = 0; i < keys; i++) {
            b.field(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        }
    });
    static final Supplier<BytesRef> OBJECT = json(b -> {
        int keys = randomIntBetween(1, 5);
        for (int i = 0; i < keys; i++) {
            b.startObject(randomAlphaOfLengthBetween(1, 10));
            int objectKeys = randomIntBetween(1, 5);
            for (int j = 0; j < objectKeys; j++) {
                b.field(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
            }
            b.endObject();
        }
    });
    static final Supplier<BytesRef> RANDOM = () -> randomFrom(EMPTY, SINGLE_KEY, MULTI_KEY, OBJECT).get();

    private static Supplier<BytesRef> json(CheckedConsumer<XContentBuilder, IOException> body) {
        return () -> {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                body.accept(builder);
                return new BytesRef(builder.endObject().toString());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
