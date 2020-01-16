/*
* Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.processor;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.xpack.ql.expression.gen.processor.BucketExtractorProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.ChainingProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.ConstantProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.HitExtractorProcessor;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.CaseProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.ConditionalProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.conditional.NullIfProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.logical.BinaryLogicProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.logical.NotProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.nulls.CheckNullProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.UnaryArithmeticProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparisonProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.InProcessor;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RegexProcessor;

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
        entries.add(new Entry(Processor.class, ChainingProcessor.NAME, ChainingProcessor::new));

        // logical
        entries.add(new Entry(Processor.class, BinaryLogicProcessor.NAME, BinaryLogicProcessor::new));
        entries.add(new Entry(Processor.class, NotProcessor.NAME, NotProcessor::new));
        // conditionals
        entries.add(new Entry(Processor.class, CaseProcessor.NAME, CaseProcessor::new));
        entries.add(new Entry(Processor.class, CheckNullProcessor.NAME, CheckNullProcessor::new));
        entries.add(new Entry(Processor.class, ConditionalProcessor.NAME, ConditionalProcessor::new));
        entries.add(new Entry(Processor.class, NullIfProcessor.NAME, NullIfProcessor::new));

        // arithmetic
        entries.add(new Entry(Processor.class, BinaryArithmeticProcessor.NAME, BinaryArithmeticProcessor::new));
        entries.add(new Entry(Processor.class, UnaryArithmeticProcessor.NAME, UnaryArithmeticProcessor::new));
        // comparators
        entries.add(new Entry(Processor.class, BinaryComparisonProcessor.NAME, BinaryComparisonProcessor::new));
        entries.add(new Entry(Processor.class, InProcessor.NAME, InProcessor::new));
        // regex
        entries.add(new Entry(Processor.class, RegexProcessor.NAME, RegexProcessor::new));

        return entries;
    }
}