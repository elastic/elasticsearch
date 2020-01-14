/*
* Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateAddProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateDiffProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DatePartProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTruncProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NamedDateTimeProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.NonIsoDateTimeProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.QuarterProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.TimeProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StDistanceProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.StWkttosqlProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryOptionalMathProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringNumericProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.BinaryStringStringProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.ConcatFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.InsertFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.LocateFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.ReplaceFunctionProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.SubstringFunctionProcessor;

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
        
        entries.addAll(org.elasticsearch.xpack.ql.expression.processor.Processors.getNamedWriteables());

        // base
        entries.add(new Entry(Processor.class, CastProcessor.NAME, CastProcessor::new));

        // datetime
        entries.add(new Entry(Processor.class, DateTimeProcessor.NAME, DateTimeProcessor::new));
        entries.add(new Entry(Processor.class, TimeProcessor.NAME, TimeProcessor::new));
        entries.add(new Entry(Processor.class, NamedDateTimeProcessor.NAME, NamedDateTimeProcessor::new));
        entries.add(new Entry(Processor.class, NonIsoDateTimeProcessor.NAME, NonIsoDateTimeProcessor::new));
        entries.add(new Entry(Processor.class, QuarterProcessor.NAME, QuarterProcessor::new));
        entries.add(new Entry(Processor.class, DateAddProcessor.NAME, DateAddProcessor::new));
        entries.add(new Entry(Processor.class, DateDiffProcessor.NAME, DateDiffProcessor::new));
        entries.add(new Entry(Processor.class, DatePartProcessor.NAME, DatePartProcessor::new));
        entries.add(new Entry(Processor.class, DateTruncProcessor.NAME, DateTruncProcessor::new));
        // math
        entries.add(new Entry(Processor.class, BinaryMathProcessor.NAME, BinaryMathProcessor::new));
        entries.add(new Entry(Processor.class, BinaryOptionalMathProcessor.NAME, BinaryOptionalMathProcessor::new));
        entries.add(new Entry(Processor.class, MathProcessor.NAME, MathProcessor::new));
        // string
        entries.add(new Entry(Processor.class, StringProcessor.NAME, StringProcessor::new));
        entries.add(new Entry(Processor.class, BinaryStringNumericProcessor.NAME, BinaryStringNumericProcessor::new));
        entries.add(new Entry(Processor.class, BinaryStringStringProcessor.NAME, BinaryStringStringProcessor::new));
        entries.add(new Entry(Processor.class, ConcatFunctionProcessor.NAME, ConcatFunctionProcessor::new));
        entries.add(new Entry(Processor.class, InsertFunctionProcessor.NAME, InsertFunctionProcessor::new));
        entries.add(new Entry(Processor.class, LocateFunctionProcessor.NAME, LocateFunctionProcessor::new));
        entries.add(new Entry(Processor.class, ReplaceFunctionProcessor.NAME, ReplaceFunctionProcessor::new));
        entries.add(new Entry(Processor.class, SubstringFunctionProcessor.NAME, SubstringFunctionProcessor::new));
        // geo
        entries.add(new Entry(Processor.class, GeoProcessor.NAME, GeoProcessor::new));
        entries.add(new Entry(Processor.class, StWkttosqlProcessor.NAME, StWkttosqlProcessor::new));
        entries.add(new Entry(Processor.class, StDistanceProcessor.NAME, StDistanceProcessor::new));
        return entries;
    }
}