/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ChainingProcessor;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.sql.execution.search.extractor.ProcessingHitExtractorTests.randomProcessor;

public class ChainingProcessorTests extends AbstractWireSerializingTestCase<ChainingProcessor> {
    public static ChainingProcessor randomComposeProcessor(int depth) {
        return new ChainingProcessor(randomProcessor(depth + 1), randomProcessor(depth + 1));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }

    @Override
    protected ChainingProcessor createTestInstance() {
        return randomComposeProcessor(0);
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
                    instance.first(), randomValueOtherThan(instance.second(), () -> randomProcessor(0))),
            () -> new ChainingProcessor(
                    randomValueOtherThan(instance.first(), () -> randomProcessor(0)), instance.second()));
        return supplier.get();
    }
}
