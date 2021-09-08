/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ql.execution.search.extractor.ComputingExtractor;
import org.elasticsearch.xpack.ql.expression.gen.processor.ChainingProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.ChainingProcessorTests;
import org.elasticsearch.xpack.ql.expression.gen.processor.HitExtractorProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.CastProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathFunctionProcessorTests;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.ql.type.DataTypes.DOUBLE;
import static org.elasticsearch.xpack.ql.util.CollectionUtils.combine;
import static org.elasticsearch.xpack.sql.util.DateUtils.UTC;

public class ComputingExtractorTests extends AbstractSqlWireSerializingTestCase<ComputingExtractor> {
    public static ComputingExtractor randomComputingExtractor() {
        return new ComputingExtractor(randomProcessor(), randomAlphaOfLength(10));
    }

    public static Processor randomProcessor() {
        List<Supplier<Processor>> options = new ArrayList<>();
        options.add(() -> ChainingProcessorTests.randomComposeProcessor());
        options.add(CastProcessorTests::randomCastProcessor);
        options.add(MathFunctionProcessorTests::randomMathFunctionProcessor);
        options.add(BinaryMathProcessorTests::randomProcessor);
        return randomFrom(options).get();
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(combine(Processors.getNamedWriteables(), SqlHitExtractors.getNamedWriteables()));
    }

    @Override
    protected ComputingExtractor createTestInstance() {
        return randomComputingExtractor();
    }

    @Override
    protected Reader<ComputingExtractor> instanceReader() {
        return ComputingExtractor::new;
    }

    @Override
    protected ComputingExtractor mutateInstance(ComputingExtractor instance) throws IOException {
        return new ComputingExtractor(
                randomValueOtherThan(instance.processor(), () -> randomProcessor()),
                randomValueOtherThan(instance.hitName(), () -> randomAlphaOfLength(10))
                );
    }

    public void testGet() {
        String fieldName = randomAlphaOfLength(5);
        ChainingProcessor extractor = new ChainingProcessor(
                new HitExtractorProcessor(new FieldHitExtractor(fieldName, DOUBLE, UTC, false)),
            new MathProcessor(MathOperation.LOG));

        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            double value = randomDouble();
            double expected = Math.log(value);
            DocumentField field = new DocumentField(fieldName, singletonList(value));
            SearchHit hit = new SearchHit(1, null, singletonMap(fieldName, field), null);
            assertEquals(expected, extractor.process(hit));
        }
    }
}
