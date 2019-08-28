/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.gen.processor;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.sql.AbstractSqlWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.sql.execution.search.extractor.ComputingExtractorTests.randomProcessor;

public class ChainingProcessorTests extends AbstractSqlWireSerializingTestCase<ChainingProcessor> {
    public static ChainingProcessor randomComposeProcessor() {
        return new ChainingProcessor(randomProcessor(), randomProcessor());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    @Override
    protected ChainingProcessor createTestInstance() {
        return randomComposeProcessor();
    }

    @Override
    protected Reader<ChainingProcessor> instanceReader() {
        return ChainingProcessor::new;
    }

    @Override
    protected ChainingProcessor mutateInstance(ChainingProcessor instance) throws IOException {
        @SuppressWarnings("unchecked")
        Supplier<ChainingProcessor> supplier = randomFrom(
            () -> new ChainingProcessor(
                    instance.first(), randomValueOtherThan(instance.second(), () -> randomProcessor())),
            () -> new ChainingProcessor(
                    randomValueOtherThan(instance.first(), () -> randomProcessor()), instance.second()));
        return supplier.get();
    }
}
