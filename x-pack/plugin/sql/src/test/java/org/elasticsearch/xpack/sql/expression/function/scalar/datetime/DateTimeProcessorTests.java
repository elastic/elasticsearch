/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.datetime;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.FunctionContext;
import org.elasticsearch.xpack.sql.expression.function.scalar.Processors;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor.DateTimeExtractor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ConstantInput;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ConstantProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.type.DataType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.TimeZone;

public class DateTimeProcessorTests extends AbstractWireSerializingTestCase<DateTimeProcessor> {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    public static DateTimeProcessor randomDateTimeProcessor() {
        return processor(DateTime.now(), randomFrom(DateTimeExtractor.values()));
    }

    @Override
    protected DateTimeProcessor createTestInstance() {
        return randomDateTimeProcessor();
    }

    @Override
    protected Reader<DateTimeProcessor> instanceReader() {
        return DateTimeProcessor::new;
    }

    @Override
    protected DateTimeProcessor mutateInstance(DateTimeProcessor instance) throws IOException {
        DateTimeExtractor replaced = randomValueOtherThan(instance.extractor(), () -> randomFrom(DateTimeExtractor.values()));
        return processor(DateTime.now(), replaced);
    }

    private static DateTimeProcessor processor(final DateTime value, final DateTimeExtractor extractor) {
        return new DateTimeProcessor(Collections.singletonList(new ConstantProcessor(value)),
            extractor,
            functionContext());
    }

    private static FunctionContext functionContext() {
        return new FunctionContext(UTC, Locale.getDefault());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Processors.getNamedWriteables());
    }
}
