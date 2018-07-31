/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.BinaryArithmeticProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.UnaryArithmeticProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.BucketExtractorProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ChainingProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.ConstantProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.HitExtractorProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.runtime.Processor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor;

import java.util.ArrayList;
import java.util.List;

public final class Processors {

    private Processors() {}

    /**
     * All of the named writeables needed to deserialize the instances of
     * {@linkplain Processors}.
     */
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        // base
        entries.add(new Entry(Processor.class, ConstantProcessor.NAME, ConstantProcessor::new));
        entries.add(new Entry(Processor.class, HitExtractorProcessor.NAME, HitExtractorProcessor::new));
        entries.add(new Entry(Processor.class, BucketExtractorProcessor.NAME, BucketExtractorProcessor::new));
        entries.add(new Entry(Processor.class, CastProcessor.NAME, CastProcessor::new));
        entries.add(new Entry(Processor.class, ChainingProcessor.NAME, ChainingProcessor::new));

        // arithmetic
        entries.add(new Entry(Processor.class, BinaryArithmeticProcessor.NAME, BinaryArithmeticProcessor::new));
        entries.add(new Entry(Processor.class, UnaryArithmeticProcessor.NAME, UnaryArithmeticProcessor::new));
        entries.add(new Entry(Processor.class, BinaryMathProcessor.NAME, BinaryMathProcessor::new));
        // datetime
        entries.add(new Entry(Processor.class, DateTimeProcessor.NAME, DateTimeProcessor::new));
        // math
        entries.add(new Entry(Processor.class, MathProcessor.NAME, MathProcessor::new));
        // string
        entries.add(new Entry(Processor.class, StringProcessor.NAME, StringProcessor::new));
        return entries;
    }
}