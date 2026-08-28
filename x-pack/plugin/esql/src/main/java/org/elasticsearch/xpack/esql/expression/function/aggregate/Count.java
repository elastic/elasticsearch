/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountAggregatorFunction;
import org.elasticsearch.compute.aggregation.DenseVectorCountAggregatorFunction;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder;
import org.elasticsearch.compute.data.HistogramBlock;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.AggregateMetricDoubleNativeSupport;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.ExtractHistogramComponent;
import org.elasticsearch.xpack.esql.expression.function.scalar.histogram.HistogramFraction;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.promql.function.PromqlFunctionDefinition;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;

public class Count extends AggregateFunction
    implements
        ToAggregator,
        SurrogateExpression,
        AggregateMetricDoubleNativeSupport,
        OptionalArgument {
    public static final TransportVersion ESQL_COUNT_HISTOGRAM_BUCKET = TransportVersion.fromName("esql_count_histogram_bucket");
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Count", Count::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Count.class)
        .binary(Count::new)
        .capabilities("flattened", "histogram_bucket")
        .name("count");
    public static final PromqlFunctionDefinition PROMQL_DEFINITION = PromqlFunctionDefinition.def()
        .acrossSeries(Count::new)
        .description("Counts the number of elements in the input vector.")
        .example("count(http_requests_total)")
        .stack(PromqlFunctionDefinition.STACK_PREVIEW_9_4_GA_9_5)
        .differenceFromPrometheus(PromqlFunctionDefinition.COUNT_NOTE)
        .name("count");

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "long",
        briefSummary = "Returns the total number of input values.",
        description = "Returns the total number (count) of input values.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "count"),
            @Example(description = "To count the number of rows, use `COUNT()` or `COUNT(*)`", file = "docs", tag = "countAll"),
            @Example(description = """
                The expression can use inline functions. This example splits a string into multiple values
                using the `SPLIT` function and counts the values.""", file = "stats", tag = "docsCountWithExpression"),
            @Example(description = """
                To count the number of times an expression returns `TRUE` use a
                [`WHERE`](/reference/query-languages/esql/commands/where.md) command to remove rows that
                shouldn’t be included.""", file = "stats", tag = "count-where"),
            @Example(
                description = "To count the number of times *multiple* expressions return `TRUE` use a WHERE inside the STATS.",
                file = "stats",
                tag = "count-where-many"
            ),
            @Example(description = """
                `COUNT`ing a multivalued field returns the number of values. `COUNT`ing `NULL` returns 0.
                `COUNT`ing `true` returns 1. `COUNT`ing `false` returns 1.""", file = "stats", tag = "count-mv"),
            @Example(description = """
                You may see a pattern like `COUNT(<expression> OR NULL)`. This has the same meaning as
                `COUNT() WHERE <expression>`. This relies on `COUNT(NULL)` to return `0` and builds on the
                three-valued logic ({wikipedia}/Three-valued_logic[3VL]): `TRUE OR NULL` is `TRUE`, but
                `FALSE OR NULL` is `NULL`. Prefer the `COUNT() WHERE <expression>` pattern.""", file = "stats", tag = "count-or-null"),
            @Example(
                description = "`COUNT` can also operate on `exponential_histogram` fields, "
                    + "returning the total number of values which were used to construct the histograms.",
                file = "exponential_histogram",
                tag = "countExpHistoForDocs"
            ),
            @Example(
                description = "`COUNT` can also operate on `tdigest` and casted `histogram` fields, "
                    + "returning the total number of values which were used to construct the digests.",
                file = "tdigest",
                tag = "countTDigestForDocs"
            ) }
    )
    public Count(
        Source source,
        @Param(
            name = "field",
            type = {
                "aggregate_metric_double",
                "boolean",
                "cartesian_point",
                "cartesian_shape",
                "exponential_histogram",
                "date",
                "date_nanos",
                "date_range",
                "dense_vector",
                "double",
                "double_range",
                "geo_point",
                "geo_shape",
                "geohash",
                "geotile",
                "geohex",
                "integer",
                "ip",
                "keyword",
                "flattened",
                "long",
                "tdigest",
                "text",
                "unsigned_long",
                "version" },
            description = "Expression that outputs values to be counted. If omitted, equivalent to `COUNT(*)` (the number of rows)."
        ) Expression field,
        @Param(
            optional = true,
            name = "bucket",
            type = { "double_range" },
            description = "Range of histogram values to count."
        ) Expression bucket
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, bucket);
    }

    public Count(Source source, Expression field) {
        this(source, field, null);
    }

    public Count(Source source, Expression field, Expression filter, Expression window) {
        this(source, field, filter, window, null);
    }

    public Count(Source source, Expression field, Expression filter, Expression window, Expression bucket) {
        super(source, field, filter, window, bucket == null ? emptyList() : List.of(bucket));
    }

    private Count(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (bucket() != null && out.getTransportVersion().supports(ESQL_COUNT_HISTOGRAM_BUCKET) == false) {
            throw new UnsupportedOperationException("version does not support count(histogram, bucket)");
        }
        super.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Count> info() {
        return NodeInfo.create(this, Count::new, field(), filter(), window(), bucket());
    }

    @Override
    public AggregateFunction withFilter(Expression filter) {
        return new Count(source(), field(), filter, window(), bucket());
    }

    @Override
    public Count replaceChildren(List<Expression> newChildren) {
        return new Count(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.size() == 3 ? null : newChildren.get(3)
        );
    }

    Expression bucket() {
        return parameters().isEmpty() ? null : parameters().getFirst();
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    public AggregatorFunctionSupplier supplier() {
        if (field().dataType() == DENSE_VECTOR) {
            return DenseVectorCountAggregatorFunction.supplier();
        }
        return CountAggregatorFunction.supplier();
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (bucket() != null) {
            return isType(
                field(),
                dt -> dt == EXPONENTIAL_HISTOGRAM || dt == DataType.TDIGEST,
                sourceText(),
                DEFAULT,
                "exponential_histogram",
                "tdigest"
            ).and(isType(bucket(), dt -> dt == DataType.DOUBLE_RANGE, sourceText(), SECOND, "double_range"));
        }
        return isType(
            field(),
            dt -> dt.isCounter() == false && dt != DataType.HISTOGRAM,
            sourceText(),
            DEFAULT,
            "any type except counter types or histogram"
        );
    }

    @Override
    protected Expression canonicalize() {
        var field = field();
        if (field.foldable() && field instanceof Literal l) {
            if (l.value() != null && ((l.value() instanceof List<?>) == false || l.dataType() == DENSE_VECTOR)) {
                // Normalize COUNT(constant) to COUNT(*) for proper deduplication.
                // This doesn't apply to COUNT([1,2,3]) which is a multi-value field.
                var wildcardLiteral = Literal.keyword(source(), StringUtils.WILDCARD);
                var canonicalFilter = filter().canonical();
                var canonicalWindow = window().canonical();
                return new Count(source(), wildcardLiteral, canonicalFilter, canonicalWindow);
            }
        }
        return super.canonicalize();
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();
        if (bucket() != null) {
            Expression count = new Coalesce(
                s,
                new ToLong(
                    s,
                    new HistogramFraction(
                        s,
                        new HistogramMerge(s, field, filter(), window()),
                        bucket(),
                        // Round cumulative counts before subtracting so errors cancel between adjacent buckets.
                        Literal.integer(s, 0)
                    )
                ),
                List.of(new Literal(s, 0L, DataType.LONG))
            );
            return new Case(s, new IsNotNull(s, bucket()), List.of(count));
        }
        if (field.dataType() == DataType.AGGREGATE_METRIC_DOUBLE) {
            return new Coalesce(s, AggregateMetricDoubleSurrogate(this), List.of(new Literal(s, 0L, DataType.LONG)));
        }

        if (field.dataType() == EXPONENTIAL_HISTOGRAM || field.dataType() == DataType.TDIGEST) {
            return new Coalesce(
                s,
                // We need to cast here because ExtractHistogramComponent returns a double.
                new ToLong(
                    s,
                    new Sum(
                        s,
                        ExtractHistogramComponent.create(source(), field, HistogramBlock.Component.COUNT),
                        filter(),
                        window(),
                        SummationMode.COMPENSATED_LITERAL
                    )
                ),
                List.of(new Literal(s, 0L, DataType.LONG))
            );
        }

        if (field.foldable()) {
            if (field instanceof Literal l) {
                if (l.value() != null && ((l.value() instanceof List<?>) == false || l.dataType() == DENSE_VECTOR)) {
                    // Does not apply to COUNT([1,2,3])
                    // return new Count(s, new Literal(s, StringUtils.WILDCARD, DataType.KEYWORD));
                    return null;
                }
            }

            // COUNT(const) is equivalent to MV_COUNT(const)*COUNT(*) if const is not null; otherwise COUNT(const) == 0.
            return new Mul(
                s,
                new Coalesce(s, new MvCount(s, field), List.of(new Literal(s, 0, DataType.INTEGER))),
                new Count(s, Literal.keyword(s, StringUtils.WILDCARD), filter(), window())
            );
        }

        return null;
    }

    public static Expression AggregateMetricDoubleSurrogate(AggregateFunction af) {
        var s = af.source();
        return new Sum(
            s,
            FromAggregateMetricDouble.withMetric(s, af.field(), AggregateMetricDoubleBlockBuilder.Metric.COUNT),
            af.filter(),
            af.window(),
            SummationMode.COMPENSATED_LITERAL
        );
    }
}
