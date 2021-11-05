/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.execution.search.extractor;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.function.Supplier;

public class ConstantExtractorTests extends AbstractWireSerializingTestCase<ConstantExtractor> {
    public static ConstantExtractor randomConstantExtractor() {
        return new ConstantExtractor(randomValidConstant());
    }

    private static Object randomValidConstant() {
        @SuppressWarnings("unchecked")
        Supplier<Object> valueSupplier = randomFrom(() -> randomInt(), () -> randomDouble(), () -> randomAlphaOfLengthBetween(1, 140));
        return valueSupplier.get();
    }

    @Override
    protected ConstantExtractor createTestInstance() {
        return randomConstantExtractor();
    }

    @Override
    protected Reader<ConstantExtractor> instanceReader() {
        return ConstantExtractor::new;
    }

    @Override
    protected ConstantExtractor mutateInstance(ConstantExtractor instance) throws IOException {
        return new ConstantExtractor(instance.extract((SearchHit) null) + "mutated");
    }

    public void testGet() {
        Object expected = randomValidConstant();
        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            assertSame(expected, new ConstantExtractor(expected).extract((SearchHit) null));
        }
    }

    public void testToString() {
        assertEquals("^foo", new ConstantExtractor("foo").toString());
        assertEquals("^42", new ConstantExtractor("42").toString());
    }
}
