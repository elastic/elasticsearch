/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.session.Result;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Cost of expanding the {@code _unmapped_fields} column of a {@code unmapped_fields="LOAD_ALL"} result into per-field columns, which
 * the coordinator does for every such query: collect the distinct field names of every row, then rewrite every page.
 * <p>
 * Each row carries {@link #FIELDS_PER_ROW} fields out of a pool of {@link #uniqueFields}, so a page of few rows holds values for only
 * a handful of the output columns. How that cost behaves as the pool grows and as the same rows are split across more, smaller pages
 * is what the parameters explore; once the pool outgrows the cap on expanded fields, the output stops growing but the names that
 * have to be sifted through do not.
 * <p>
 * Run with {@code ./gradlew :x-pack:plugin:esql:benchmark --args "ExpandUnmappedFieldsPostProcessorBenchmark"}, adding
 * {@code -prof gc} to see allocation.
 */
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(1)
public class ExpandUnmappedFieldsPostProcessorBenchmark {
    static {
        BenchmarkLogging.configure();
    }

    private static final int FIELDS_PER_ROW = 10;

    private static final BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("none"))
        .build();

    /** Distinct fields across all rows. Past {@code MAX_EXPANDED_FIELDS} only the alphabetically first are expanded. */
    @Param({ "10", "1000", "100000" })
    public int uniqueFields;

    /** Rows in every page. The total number of rows is {@code pages * rowsPerPage}. */
    @Param({ "1", "10", "1000" })
    public int rowsPerPage;

    @Param({ "1", "100" })
    public int pages;

    private Result input;

    /** The expansion consumes its input, so every invocation needs a fresh one. */
    @Setup(Level.Invocation)
    public void setup() {
        List<Attribute> schema = List.of(
            new ReferenceAttribute(Source.EMPTY, null, "id", DataType.INTEGER),
            new UnmappedFieldsAttribute(Source.EMPTY, UnmappedFieldsPattern.ALL)
        );
        List<Page> inputPages = new ArrayList<>(pages);
        int row = 0;
        for (int p = 0; p < pages; p++) {
            List<List<Object>> rows = new ArrayList<>(rowsPerPage);
            for (int r = 0; r < rowsPerPage; r++, row++) {
                rows.add(List.of(row, json(row)));
            }
            inputPages.add(new Page(BlockUtils.fromList(blockFactory, rows)));
        }
        // The expansion passes the configuration through without reading it.
        input = new Result(schema, inputPages, Map.of(), null, DriverCompletionInfo.EMPTY, null, null);
    }

    /** {@link #FIELDS_PER_ROW} consecutive fields of the pool, wrapping around, starting where the previous row left off. */
    private String json(int row) {
        StringBuilder json = new StringBuilder("{");
        for (int i = 0; i < FIELDS_PER_ROW; i++) {
            if (i > 0) {
                json.append(',');
            }
            int field = (row * FIELDS_PER_ROW + i) % uniqueFields;
            json.append("\"f").append(String.format(Locale.ROOT, "%06d", field)).append("\":").append(i);
        }
        return json.append('}').toString();
    }

    @Benchmark
    public void expand(Blackhole blackhole) {
        Result expanded = ExpandUnmappedFieldsPostProcessor.expand(input, null, blockFactory, PlannerSettings.DEFAULTS, () -> false);
        blackhole.consume(expanded.schema().size());
        Releasables.close(expanded.pages());
    }
}
