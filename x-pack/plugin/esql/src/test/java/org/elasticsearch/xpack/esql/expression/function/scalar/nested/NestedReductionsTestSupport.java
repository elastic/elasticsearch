/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.EmptyIndexedByShardId;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.lookup.RightChunkedLeftJoin;
import org.elasticsearch.compute.querydsl.query.QueryWarnings;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineLimits;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReorderLimitProjectAndOrderBy;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.ReplaceLimitAndSortAsTopN;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.PlanWritables;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.OutputExec;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.LocalExecutionPlanner;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.session.Versioned;
import org.elasticsearch.xpack.esql.telemetry.Metrics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

/** Exercises real analysis, optimization, mapping and compute; only index scans use supplied rows. */
final class NestedReductionsTestSupport {
    private NestedReductionsTestSupport() {}

    static LogicalPlan analyze(String query, EsIndex index, Map<String, IndexResolution> lookups) {
        return analyze(query, Map.of(index.name(), index), lookups);
    }

    static LogicalPlan analyze(String query, Map<String, EsIndex> indices, Map<String, IndexResolution> lookups) {
        var registry = new EsqlFunctionRegistry().snapshotRegistry();
        var statement = new EsqlParser(new EsqlConfig(registry)).createStatement(query);
        var config = configuration(query);
        Map<IndexPattern, IndexResolution> resolutions = new LinkedHashMap<>();
        indices.forEach((name, index) -> resolutions.put(new IndexPattern(Source.EMPTY, name), IndexResolution.valid(index)));
        var context = new AnalyzerContext(
            config,
            registry,
            PromqlFunctionRegistry.INSTANCE,
            null,
            resolutions,
            lookups,
            new EnrichResolution(),
            InferenceResolution.EMPTY,
            TransportVersion.current(),
            statement.setting(QuerySettings.UNMAPPED_FIELDS)
        );
        return new Analyzer(context, new Verifier(new Metrics(registry, true, false), new XPackLicenseState(() -> 0L))).analyze(
            statement.plan()
        );
    }

    static LogicalPlan optimize(String query, LogicalPlan analyzed) {
        var optimizerContext = new LogicalOptimizerContext(
            configuration(query),
            FoldContext.small(),
            TransportVersion.current(),
            EsqlFlags.DEFAULTS
        );
        return new LogicalPlanOptimizer(optimizerContext).optimize(analyzed);
    }

    static LogicalPlan roundTrip(String query, LogicalPlan plan) throws IOException {
        var config = configuration(query);
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(ExpressionWritables.getNamedWriteables());
        entries.addAll(PlanWritables.getNamedWriteables());
        if (plan.anyMatch(MergePlan.class::isInstance)) {
            // Logical merges are coordinator-only and have no wire format. Validate their reduction payloads separately.
            return plan.transformExpressionsUp(NestedReduction.class, expression -> {
                try (var bytes = new BytesStreamOutput(); var output = new PlanStreamOutput(bytes, config)) {
                    output.writeNamedWriteable(expression);
                    var externalIds = expression.children()
                        .stream()
                        .map(Attribute.class::cast)
                        .map(attr -> Long.valueOf(attr.id().toString()))
                        .collect(java.util.stream.Collectors.toSet());
                    var ids = new PlanStreamInput.NameIdMapper() {
                        @Override
                        public NameId apply(long id) {
                            // Keep the enclosing coordinator layout, but remap private child inputs as a real wire read does.
                            return externalIds.contains(id) ? new NameId(id) : super.apply(id);
                        }
                    };
                    try (var input = new PlanStreamInput(bytes.bytes().streamInput(), new NamedWriteableRegistry(entries), config, ids)) {
                        return (NestedReduction) input.readNamedWriteable(Expression.class);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
        try (var bytes = new BytesStreamOutput(); var output = new PlanStreamOutput(bytes, config)) {
            output.writeNamedWriteable(plan);
            try (var input = new PlanStreamInput(bytes.bytes().streamInput(), new NamedWriteableRegistry(entries), config)) {
                return input.readNamedWriteable(LogicalPlan.class);
            }
        }
    }

    private static Configuration configuration(String query) {
        return new Configuration(
            Instant.EPOCH,
            Locale.ROOT,
            "test",
            "test",
            QueryPragmas.EMPTY,
            10000,
            1000,
            query,
            false,
            Map.of(),
            0,
            false,
            10000,
            1000,
            null,
            Map.of()
        );
    }

    static List<List<Object>> runLocal(
        String query,
        LogicalPlan analyzed,
        ToIntFunction<EsRelation> rowCount,
        BiFunction<EsRelation, Attribute, List<?>> input,
        boolean convertValues,
        boolean fullyOptimize
    ) throws Exception {
        var fold = FoldContext.small();
        var config = configuration(query);
        var factory = new BlockFactory(new NoopCircuitBreaker("nested-test"), BigArrays.NON_RECYCLING_INSTANCE);
        List<Page> sourcePages = new ArrayList<>();
        try {
            LogicalPlan materialized = analyzed.transformUp(p -> {
                try {
                    if (p instanceof MergePlan merge) {
                        List<List<Object>> merged = new ArrayList<>();
                        for (LogicalPlan branch : merge.children()) {
                            var values = runLocal(query, branch, rowCount, input, false, fullyOptimize);
                            for (var row : values) {
                                List<Object> ordered = new ArrayList<>();
                                for (Attribute attr : merge.output()) {
                                    int c = IntStream.range(0, branch.output().size())
                                        .filter(i -> branch.output().get(i).name().equals(attr.name()))
                                        .findFirst()
                                        .orElseThrow();
                                    ordered.add(row.get(c));
                                }
                                merged.add(ordered);
                            }
                        }
                        Page page = fromRows(factory, merge.output(), merged);
                        sourcePages.add(page);
                        return new LocalRelation(Source.EMPTY, merge.output(), new CopyingLocalSupplier(page));
                    }
                    if (p instanceof Join join) {
                        var left = runLocal(query, join.left(), rowCount, input, false, fullyOptimize);
                        var right = runLocal(query, join.right(), rowCount, input, false, fullyOptimize);
                        List<Attribute> added = join.rightOutputFields();
                        List<List<Object>> matches = new ArrayList<>();
                        for (int l = 0; l < left.size(); l++) {
                            for (var r : right) {
                                boolean matched = true;
                                for (int k = 0; k < join.config().leftFields().size(); k++) {
                                    Object key = left.get(l).get(column(join.left().output(), join.config().leftFields().get(k)));
                                    Object other = r.get(column(join.right().output(), join.config().rightFields().get(k)));
                                    matched &= key != null && key.equals(other);
                                }
                                if (matched) {
                                    List<Object> values = new ArrayList<>();
                                    values.add(l);
                                    for (Attribute attr : added) {
                                        values.add(r.get(column(join.right().output(), attr)));
                                    }
                                    matches.add(values);
                                }
                            }
                        }
                        List<List<Object>> joined = new ArrayList<>();
                        var positions = new ReferenceAttribute(Source.EMPTY, "positions", DataType.INTEGER);
                        List<Attribute> rightSchema = new ArrayList<>();
                        rightSchema.add(positions);
                        rightSchema.addAll(added);
                        try (var lookup = new RightChunkedLeftJoin(fromRows(factory, join.left().output(), left), added.size())) {
                            if (matches.isEmpty() == false) {
                                var results = fromRows(factory, rightSchema, matches);
                                try {
                                    appendRows(lookup.join(results), joined);
                                } finally {
                                    results.releaseBlocks();
                                }
                            }
                            lookup.noMoreRightHandPages().ifPresent(page -> appendRows(page, joined));
                        }
                        List<Attribute> joinedSchema = new ArrayList<>(join.left().output());
                        joinedSchema.addAll(added);
                        List<List<Object>> ordered = joined.stream()
                            .map(row -> join.output().stream().map(attr -> row.get(column(joinedSchema, attr))).toList())
                            .toList();
                        Page page = fromRows(factory, join.output(), ordered);
                        sourcePages.add(page);
                        return new LocalRelation(Source.EMPTY, join.output(), new CopyingLocalSupplier(page));
                    }
                    return p;
                } catch (Exception e) {
                    throw new IllegalStateException("materializing local relational test input", e);
                }
            });
            var optimizerContext = new LogicalOptimizerContext(config, fold, TransportVersion.current(), EsqlFlags.DEFAULTS);
            var optimizer = fullyOptimize ? new LogicalPlanOptimizer(optimizerContext) : new LogicalPlanOptimizer(optimizerContext) {
                @Override
                protected List<Batch<LogicalPlan>> batches() {
                    // Planning substitutions and sort/limit lowering remain required; omit folding, pruning and predicate rewrites.
                    return List.of(
                        substitutions(),
                        new Batch<>(
                            "Required planning lowerings",
                            new PushDownAndCombineLimits(),
                            new ReorderLimitProjectAndOrderBy(),
                            new ReplaceLimitAndSortAsTopN()
                        )
                    );
                }
            };
            var optimized = optimizer.optimize(materialized);
            var local = optimized.transformUp(EsRelation.class, relation -> {
                List<Block> blocks = new ArrayList<>();
                for (Attribute attr : relation.output()) {
                    try (
                        var wrapper = BlockUtils.wrapperFor(
                            factory,
                            PlannerUtils.toElementType(attr.dataType()),
                            rowCount.applyAsInt(relation)
                        )
                    ) {
                        for (Object value : input.apply(relation, attr)) {
                            wrapper.accept(value);
                        }
                        blocks.add(wrapper.builder().build());
                    }
                }
                var page = new Page(rowCount.applyAsInt(relation), blocks.toArray(Block[]::new));
                sourcePages.add(page);
                return new LocalRelation(Source.EMPTY, relation.output(), new CopyingLocalSupplier(page));
            });
            List<List<Object>> rows = new ArrayList<>();
            var physical = new Mapper().map(new Versioned<>(local, TransportVersion.current()));
            if (optimized.expressions().stream().anyMatch(e -> e.anyMatch(NestedReduction.class::isInstance))
                && physical.anyMatch(org.elasticsearch.xpack.esql.plan.physical.NestedReduceExec.class::isInstance) == false) {
                throw new AssertionError("lost scoped physical reduction boundary");
            }
            var shards = EmptyIndexedByShardId.<EsPhysicalOperationProviders.ShardContext>instance();
            var settings = Settings.builder().put("node.name", "nested-test").build();
            var provider = new EsPhysicalOperationProviders(
                fold,
                config,
                shards,
                null,
                PlannerSettings.DEFAULTS,
                () -> 0L,
                QueryWarnings.EMIT
            );
            var planner = new LocalExecutionPlanner(
                "test",
                "",
                null,
                BigArrays.NON_RECYCLING_INSTANCE,
                factory,
                settings,
                config,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                provider,
                null,
                true,
                null,
                0,
                MatcherWatchdog.noop(),
                TransportVersion.current(),
                null
            );
            var output = new OutputExec(EstimatesRowSize.estimateRowSize(0, physical), page -> {
                try {
                    if (convertValues) {
                        var converted = ResponseValueUtils.pagesToValues(
                            physical.output().stream().map(Attribute::dataType).toList(),
                            List.of(page),
                            ZoneOffset.UTC
                        );
                        converted.forEachRemaining(values -> {
                            List<Object> row = new ArrayList<>();
                            values.forEachRemaining(row::add);
                            rows.add(row);
                        });
                    } else {
                        for (int i = 0; i < page.getPositionCount(); i++) {
                            List<Object> row = new ArrayList<>();
                            for (int c = 0; c < page.getBlockCount(); c++) {
                                row.add(BlockUtils.toJavaObject(page.getBlock(c), i));
                            }
                            rows.add(row);
                        }
                    }
                } finally {
                    page.releaseBlocks();
                }
            });
            var execution = planner.plan("nested-test", fold, PlannerSettings.DEFAULTS, output, shards, true);
            var threadContext = new ThreadContext(settings);
            {
                for (var driver : execution.createDrivers("test")) {
                    var done = new PlainActionFuture<Void>();
                    Driver.start(threadContext, Runnable::run, driver, 10000, done);
                    done.actionGet();
                }
            }
            return rows;
        } finally {
            sourcePages.forEach(Page::releaseBlocks);
        }
    }

    private static int column(List<Attribute> schema, Attribute attr) {
        return IntStream.range(0, schema.size()).filter(i -> schema.get(i).id().equals(attr.id())).findFirst().orElseThrow();
    }

    private static Page fromRows(BlockFactory factory, List<Attribute> schema, List<List<Object>> rows) {
        List<Block> blocks = new ArrayList<>();
        for (int c = 0; c < schema.size(); c++) {
            try (var wrapper = BlockUtils.wrapperFor(factory, PlannerUtils.toElementType(schema.get(c).dataType()), rows.size())) {
                for (var row : rows) {
                    wrapper.accept(row.get(c));
                }
                blocks.add(wrapper.builder().build());
            }
        }
        return new Page(rows.size(), blocks.toArray(Block[]::new));
    }

    private static void appendRows(Page page, List<List<Object>> rows) {
        try {
            for (int i = 0; i < page.getPositionCount(); i++) {
                List<Object> row = new ArrayList<>();
                for (int c = 0; c < page.getBlockCount(); c++) {
                    row.add(BlockUtils.toJavaObject(page.getBlock(c), i));
                }
                rows.add(row);
            }
        } finally {
            page.releaseBlocks();
        }
    }

}
