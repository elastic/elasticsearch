/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.gen.processor;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.BinaryLogicProcessorTests;
import org.elasticsearch.xpack.esql.core.expression.processor.Processors;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/109012")
public class ChainingProcessorTests extends AbstractWireSerializingTestCase<ChainingProcessor> {
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
    protected ChainingProcessor mutateInstance(ChainingProcessor instance) {
        @SuppressWarnings("unchecked")
        Supplier<ChainingProcessor> supplier = randomFrom(
            () -> new ChainingProcessor(instance.first(), randomValueOtherThan(instance.second(), () -> randomProcessor())),
            () -> new ChainingProcessor(randomValueOtherThan(instance.first(), () -> randomProcessor()), instance.second())
        );
        return supplier.get();
    }

    public static Processor randomProcessor() {
        List<Supplier<Processor>> options = new ArrayList<>();
        options.add(ChainingProcessorTests::randomComposeProcessor);
        options.add(BinaryLogicProcessorTests::randomProcessor);
        return randomFrom(options).get();
    }
}
