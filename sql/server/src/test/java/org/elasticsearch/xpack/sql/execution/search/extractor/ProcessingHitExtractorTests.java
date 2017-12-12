/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.CastProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathFunctionProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ChainingProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ChainingProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.HitExtractorProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.MatrixFieldProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

public class ProcessingHitExtractorTests extends AbstractWireSerializingTestCase<ComputingHitExtractor> {
    public static ComputingHitExtractor randomProcessingHitExtractor(int depth) {
        return new ComputingHitExtractor(randomProcessor(0));
    }

    public static Processor randomProcessor(int depth) {
        List<Supplier<Processor>> options = new ArrayList<>();
        if (depth < 5) {
            options.add(() -> ChainingProcessorTests.randomComposeProcessor(depth));
        }
        options.add(CastProcessorTests::randomCastProcessor);
        options.add(DateTimeProcessorTests::randomDateTimeProcessor);
        options.add(MathFunctionProcessorTests::randomMathFunctionProcessor);
        options.add(MatrixFieldProcessorTests::randomMatrixFieldProcessor);
        return randomFrom(options).get();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(HitExtractors.getNamedWriteables());
    }

    @Override
    protected ComputingHitExtractor createTestInstance() {
        return randomProcessingHitExtractor(0);
    }

    @Override
    protected Reader<ComputingHitExtractor> instanceReader() {
        return ComputingHitExtractor::new;
    }

    @Override
    protected ComputingHitExtractor mutateInstance(ComputingHitExtractor instance) throws IOException {
        return new ComputingHitExtractor(
                randomValueOtherThan(instance.processor(), () -> randomProcessor(0)));
    }

    public void testGet() {
        String fieldName = randomAlphaOfLength(5);
        ChainingProcessor extractor = new ChainingProcessor(new HitExtractorProcessor(new DocValueExtractor(fieldName)), new MathProcessor(MathOperation.LOG));

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            double value = randomDouble();
            double expected = Math.log(value);
            SearchHit hit = new SearchHit(1);
            DocumentField field = new DocumentField(fieldName, singletonList(value));
            hit.fields(singletonMap(fieldName, field));
            assertEquals(expected, extractor.process(hit));
        }
    }
}
