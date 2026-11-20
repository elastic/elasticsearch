/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerSettings;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.IpLocationResolution;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Reproducible;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.inference.InferenceResolution;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.Layout;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * A scalar ES|QL expression over fields, evaluated outside of a query, for example on the documents of a segment. It runs with
 * a fixed configuration (UTC, {@link Locale#ROOT}) and only uses {@link Reproducible} functions, so it returns the same values
 * wherever and whenever it runs.
 */
public final class FieldExpression {

    private static final EsqlFunctionRegistry REGISTRY = new EsqlFunctionRegistry();

    private static final Configuration CONFIGURATION = new Configuration(
        Instant.EPOCH,
        Locale.ROOT,
        null,
        null,
        new QueryPragmas(Settings.EMPTY),
        AnalyzerSettings.QUERY_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
        AnalyzerSettings.QUERY_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
        "",
        false,
        Map.of(),
        0L,
        false,
        AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_MAX_SIZE.getDefault(Settings.EMPTY),
        AnalyzerSettings.QUERY_TIMESERIES_RESULT_TRUNCATION_DEFAULT_SIZE.getDefault(Settings.EMPTY),
        ResolvedSettings.EMPTY,
        Map.of()
    );

    private static final String INDEX = "fields";
    private static final String KEY = "key";

    private static final AnalyzerContext ANALYZER_CONTEXT = new AnalyzerContext(
        CONFIGURATION,
        REGISTRY,
        PromqlFunctionRegistry.INSTANCE,
        null,
        null,
        Map.of(),
        Map.of(),
        Map.of(),
        new EnrichResolution(),
        InferenceResolution.EMPTY,
        ExternalSourceResolution.EMPTY,
        TransportVersion.current(),
        UnmappedResolution.DEFAULT,
        null,
        IpLocationResolution.SERVICE_UNAVAILABLE,
        false,
        Map.of(),
        EsqlFlags.DEFAULTS,
        Map.of()
    );

    /** Only aggregations check the license, and expressions can't use them. */
    private static final Verifier VERIFIER = new Verifier(null, new XPackLicenseState(System::currentTimeMillis));

    private final DataType type;
    private final ExpressionEvaluator.Factory evaluator;

    private FieldExpression(DataType type, ExpressionEvaluator.Factory evaluator) {
        this.type = type;
        this.evaluator = evaluator;
    }

    /** A parser that knows the functions that expressions can use. */
    public static EsqlParser parser() {
        return new EsqlParser(new EsqlConfig(REGISTRY));
    }

    /**
     * Checks a parsed expression before the types of its fields are known: it reads fields and only uses {@link Reproducible}
     * functions that can be computed for each row.
     *
     * @return the fields the expression reads, in order
     * @throws IllegalArgumentException if the expression is invalid
     */
    public static Set<String> fields(Expression expression) {
        Set<String> fields = new LinkedHashSet<>();
        expression.forEachDown(n -> {
            if (n instanceof UnresolvedAttribute a) {
                fields.add(a.name());
            } else if (n instanceof UnresolvedFunction f) {
                String name = REGISTRY.resolveAlias(f.name());
                if (REGISTRY.functionExists(name) == false) {
                    throw new IllegalArgumentException("uses unknown function [" + f.name() + "]");
                }
                checkFunction(n, REGISTRY.resolveFunction(name).clazz());
            } else if (n instanceof Function) {
                checkFunction(n, n.getClass());
            }
        });
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("must read at least one field");
        }
        return fields;
    }

    private static void checkFunction(Expression node, Class<?> function) {
        if (EvaluatorMapper.class.isAssignableFrom(function) == false
            || AggregateFunction.class.isAssignableFrom(function)
            || FullTextFunction.class.isAssignableFrom(function)) {
            throw new IllegalArgumentException("uses [" + node.sourceText() + "], which can't be computed for each document");
        }
        if (Reproducible.class.isAssignableFrom(function) == false) {
            throw new IllegalArgumentException(
                "uses [" + node.sourceText() + "], which isn't guaranteed to compute the same values on every node and version"
            );
        }
    }

    /**
     * Resolves a parsed expression over fields of the given types and builds its evaluator, whose input pages hold the fields
     * in the order of {@code fieldTypes}. ES|QL analyzes and optimizes the expression as {@code EVAL key = expression} on a
     * relation of these fields.
     *
     * @throws IllegalArgumentException if the expression is invalid for these types
     */
    public static FieldExpression compile(Expression expression, Map<String, DataType> fieldTypes) {
        fields(expression);
        Source source = expression.source();
        List<Attribute> fields = new ArrayList<>();
        fieldTypes.forEach(
            (name, type) -> fields.add(
                new FieldAttribute(source, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.UNKNOWN))
            )
        );
        // A relation on an index, which the optimizer doesn't replace with an empty one, and an explicit limit, which the
        // analyzer doesn't add with a warning
        EsRelation relation = new EsRelation(
            source,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, new IndexProperties(IndexMode.STANDARD, 1, IndexVersion.current().id())),
            fields
        );
        Eval eval = new Eval(source, relation, List.of(new Alias(source, KEY, expression)));
        LogicalPlan plan = new Limit(source, new Literal(source, 1, DataType.INTEGER), eval);
        // Each expression has its own budget for folding constants
        FoldContext foldContext = FoldContext.small();
        LogicalPlan optimized;
        try {
            LogicalPlan analyzed = new Analyzer(ANALYZER_CONTEXT, VERIFIER).analyze(plan);
            optimized = new LogicalPlanOptimizer(
                new LogicalOptimizerContext(CONFIGURATION, foldContext, TransportVersion.current(), EsqlFlags.DEFAULTS)
            ).optimize(analyzed);
        } catch (VerificationException e) {
            // Keeps the problems, without their positions in the plan that holds the expression
            String problems = e.getMessage().lines().skip(1).map(l -> l.replaceFirst("^line \\d+:\\d+: ", "")).collect(joining("; "));
            throw new IllegalArgumentException(problems, e);
        }
        Holder<Expression> resolved = new Holder<>();
        optimized.forEachExpressionDown(Alias.class, alias -> {
            if (alias.name().equals(KEY)) {
                resolved.set(alias.child());
            }
        });
        if (resolved.get() == null) {
            throw new IllegalStateException("[" + expression.sourceText() + "] is missing from the optimized plan [" + optimized + "]");
        }
        // The analyzer and optimizer may add functions, such as implicit casts
        resolved.get().forEachDown(n -> {
            if ((n instanceof FieldAttribute || n instanceof Literal) == false) {
                checkFunction(n, n.getClass());
            }
        });
        Layout.Builder layout = new Layout.Builder();
        fields.forEach(layout::append);
        ExpressionEvaluator.Factory evaluator = EvalMapper.toEvaluator(foldContext, resolved.get(), layout.build());
        return new FieldExpression(resolved.get().dataType(), evaluator);
    }

    public DataType type() {
        return type;
    }

    public ExpressionEvaluator.Factory evaluator() {
        return evaluator;
    }
}
