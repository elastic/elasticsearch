/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.sql.execution.search.ProcessingHitExtractorTests.randomColumnProcessor;

public class ComposeProcessorTests extends AbstractWireSerializingTestCase<ComposeProcessor> {
    public static ComposeProcessor randomComposeProcessor(int depth) {
        return new ComposeProcessor(randomColumnProcessor(depth + 1), randomColumnProcessor(depth + 1));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ColumnProcessor.getNamedWriteables());
    }

    @Override
    protected ComposeProcessor createTestInstance() {
        return randomComposeProcessor(0);
    }

    @Override
    protected Reader<ComposeProcessor> instanceReader() {
        return ComposeProcessor::new;
    }

    @Override
    protected ComposeProcessor mutateInstance(ComposeProcessor instance) throws IOException {
        @SuppressWarnings("unchecked")
        Supplier<ComposeProcessor> supplier = randomFrom(
            () -> new ComposeProcessor(
                    instance.first(), randomValueOtherThan(instance.second(), () -> randomColumnProcessor(0))),
            () -> new ComposeProcessor(
                    randomValueOtherThan(instance.first(), () -> randomColumnProcessor(0)), instance.second()));
        return supplier.get();
    }
}
