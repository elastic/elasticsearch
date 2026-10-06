/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.terms;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures building a {@code long} field terms query from a list of boxed values, which is what
 * {@code TermsQueryBuilder} hands to {@link NumberType#termsQuery} on every shard request.
 * <p>
 * Values parsed from a JSON request arrive as {@link Integer} when they fit in an int, so
 * {@code INTEGER} is the common case for id lists on {@code long} fields, and {@code LONG} is the
 * baseline that already had a direct path in {@link NumberType#objectToLong}. Run with
 * {@code -prof gc} to see allocation as well as time. Each invocation converts the whole list, so
 * {@code gc.alloc.rate.norm} is bytes per list; divide by {@code nTerms} for bytes per value.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class LongTermsQueryBuildBenchmark {

    static final String FIELD = "f";

    /** The boxed type the query values arrive as. */
    public enum ValueType {
        INTEGER {
            @Override
            Object box(int value) {
                return value;
            }
        },
        LONG {
            @Override
            Object box(int value) {
                return (long) value;
            }
        };

        abstract Object box(int value);
    }

    @Param({ "INTEGER", "LONG" })
    public ValueType valueType;

    @Param({ "100", "10000", "100000" })
    public int nTerms;

    private List<Object> values;

    @Setup(Level.Trial)
    public void setupTrial() {
        Random random = new Random(42);
        values = new ArrayList<>(nTerms);
        for (int i = 0; i < nTerms; i++) {
            values.add(valueType.box(random.nextInt(Integer.MAX_VALUE)));
        }
    }

    @Benchmark
    public Query termsQuery() {
        return NumberType.LONG.termsQuery(FIELD, values);
    }

    @Benchmark
    public long objectToLong() {
        long sum = 0;
        for (Object value : values) {
            sum += NumberType.objectToLong(value, true);
        }
        return sum;
    }
}
