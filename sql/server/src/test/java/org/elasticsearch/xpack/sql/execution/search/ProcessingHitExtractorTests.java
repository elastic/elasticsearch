/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.CastProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.ColumnProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.ComposeProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.DateTimeProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.MathFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.MathFunctionProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.MatrixFieldProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class ProcessingHitExtractorTests extends AbstractWireSerializingTestCase<ProcessingHitExtractor> {
    public static ProcessingHitExtractor randomProcessingHitExtractor(int depth) {
        return new ProcessingHitExtractor(ScrollCursorTests.randomHitExtractor(depth + 1), randomColumnProcessor(0));
    }

    public static ColumnProcessor randomColumnProcessor(int depth) {
        List<Supplier<ColumnProcessor>> options = new ArrayList<>();
        if (depth < 5) {
            options.add(() -> ComposeProcessorTests.randomComposeProcessor(depth));
        }
        options.add(CastProcessorTests::randomCastProcessor);
        options.add(DateTimeProcessorTests::randomDateTimeProcessor);
        options.add(MathFunctionProcessorTests::randomMathFunctionProcessor);
        options.add(MatrixFieldProcessorTests::randomMatrixFieldProcessor);
        return randomFrom(options).get();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(HitExtractor.getNamedWriteables());
    }

    @Override
    protected ProcessingHitExtractor createTestInstance() {
        return randomProcessingHitExtractor(0);
    }

    @Override
    protected Reader<ProcessingHitExtractor> instanceReader() {
        return ProcessingHitExtractor::new;
    }

    @Override
    protected ProcessingHitExtractor mutateInstance(ProcessingHitExtractor instance) throws IOException {
        @SuppressWarnings("unchecked")
        Supplier<ProcessingHitExtractor> supplier = randomFrom(
                () -> new ProcessingHitExtractor(
                        randomValueOtherThan(instance.delegate(), () -> ScrollCursorTests.randomHitExtractor(0)),
                        instance.processor()),
                () -> new ProcessingHitExtractor(
                        instance.delegate(),
                        randomValueOtherThan(instance.processor(), () -> randomColumnProcessor(0))));
        return supplier.get();
    }

    public void testGet() {
        String fieldName = randomAlphaOfLength(5);
        ProcessingHitExtractor extractor = new ProcessingHitExtractor(
                new DocValueExtractor(fieldName), new MathFunctionProcessor(MathProcessor.LOG));

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            double value = randomDouble();
            double expected = Math.log(value);
            SearchHit hit = new SearchHit(1);
            DocumentField field = new DocumentField(fieldName, singletonList(value));
            hit.fields(singletonMap(fieldName, field));
            assertEquals(expected, extractor.get(hit));
        }
    }
}
