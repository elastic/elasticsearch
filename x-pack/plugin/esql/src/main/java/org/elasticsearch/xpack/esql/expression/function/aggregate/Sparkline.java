/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.SparklineAggregatorFunction;
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.capabilities.ConfigurationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.AggregateMetricDoubleNativeSupport;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.AGGREGATE_METRIC_DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.EXPONENTIAL_HISTOGRAM;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToLong;

public class Sparkline extends AggregateFunction implements ToAggregator, AggregateMetricDoubleNativeSupport {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Sparkline",
        Sparkline::new
    );
    private final Expression key;
    private final Expression buckets;
    private final Expression from;
    private final Expression to;

    @FunctionInfo(
        returnType = "double",
        description = "The average of a numeric field.",
        type = FunctionType.AGGREGATE,
        examples = {
            @Example(file = "stats", tag = "avg"),
            @Example(
                description = "The expression can use inline functions. For example, to calculate the average "
                    + "over a multivalued column, first use `MV_AVG` to average the multiple values per row, "
                    + "and use the result with the `AVG` function",
                file = "stats",
                tag = "docsStatsAvgNestedExpression"
            ) }
    )
    public Sparkline(
        Source source,
        @Param(
            name = "number",
            type = { "aggregate_metric_double", "exponential_histogram", "tdigest", "double", "integer", "long" },
            description = "Expression that outputs values to average."
        ) Expression field,
        @Param(
            name = "key",
            type = { "keyword", "text" },
            description = "Expression that outputs the key for the sparkline."
        ) Expression key,
        Expression buckets,
        Expression from,
        Expression to
    ) {
        this(source, field, Literal.TRUE, NO_WINDOW, key, buckets, from, to);
    }

    public Sparkline(
        Source source,
        Expression field,
        Expression filter,
        Expression window,
        Expression key,
        Expression buckets,
        Expression from,
        Expression to
    ) {
        super(source, field, filter, window, List.of(key, buckets, from, to));
        this.key = key;
        this.buckets = buckets;
        this.from = from;
        this.to = to;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }
        return isType(
            field(),
            dt -> (dt.isNumeric() && dt != DataType.UNSIGNED_LONG)
                || dt == AGGREGATE_METRIC_DOUBLE
                || dt == EXPONENTIAL_HISTOGRAM
                || dt == DataType.TDIGEST,
            sourceText(),
            DEFAULT,
            "aggregate_metric_double, exponential_histogram, tdigest or numeric except unsigned_long or counter types"
        );
    }

    private Sparkline(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            readWindow(in)
        ); // TODO: Update this
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    protected NodeInfo<Sparkline> info() {
        return NodeInfo.create(this, Sparkline::new, field(), key, filter(), window(), buckets, from, to);
    }

    @Override
    public Sparkline replaceChildren(List<Expression> newChildren) {
        return new Sparkline(
            source(),
            newChildren.get(0),
            newChildren.get(1),
            newChildren.get(2),
            newChildren.get(3),
            newChildren.get(4),
            newChildren.get(5),
            newChildren.get(6)
        );
    }

    @Override
    public Sparkline withFilter(Expression filter) {
        return new Sparkline(source(), field(), key, filter, window(), buckets, from, to);
    }

    @Override
    public final AggregatorFunctionSupplier supplier() {
        DataType type = field().dataType();
        if (type != LONG) {
            // If the type checking did its job, this should never happen
            throw EsqlIllegalArgumentException.illegalDataType(type);
        }

        AggregatorFunctionSupplier supplier = null;
        if (field() instanceof ToAggregator toAggregator) {
            supplier = toAggregator.supplier();
        } else {
            throw new ElasticsearchStatusException(
                Strings.format("Cannot create aggregator for [{}] of type [{}]", getWriteableName(), field().dataType()),
                RestStatus.INTERNAL_SERVER_ERROR
            );
        }

        var bucketExpr = new Bucket(source(), key, buckets, from, to, ConfigurationAware.CONFIGURATION_MARKER);
        var rounding = bucketExpr.getDateRounding(FoldContext.small(), null, null);
        var minDate = foldToLong(FoldContext.small(), bucketExpr.from());
        var maxDate = foldToLong(FoldContext.small(), bucketExpr.to());
        return new SparklineAggregatorFunction.SparklineAggregatorFunctionSupplier(rounding, minDate, maxDate, supplier);
    }

    private long foldToLong(FoldContext ctx, Expression e) {
        Object value = Foldables.valueOf(ctx, e);
        return DataType.isDateTime(e.dataType()) ? ((Number) value).longValue() : dateTimeToLong(((BytesRef) value).utf8ToString());
    }

    @Override
    public boolean resolved() {
        return key.resolved() && buckets.resolved();
    }
}
