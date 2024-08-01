/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctBooleanAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctBytesRefAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctDoubleAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctIntAggregatorFunctionSupplier;
import org.elasticsearch.compute.aggregation.CountDistinctLongAggregatorFunctionSupplier;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvCount;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvDedupe;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isWholeNumber;

public class CountDistinct extends AggregateFunction implements OptionalArgument, ToAggregator, SurrogateExpression {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "CountDistinct",
        CountDistinct::new
    );

    private static final int DEFAULT_PRECISION = 3000;
    private final Expression precision;

    @FunctionInfo(
        returnType = "long",
        description = "Returns the approximate number of distinct values.",
        appendix = """
            [discrete]
            [[esql-agg-count-distinct-approximate]]
            ==== Counts are approximate

            Computing exact counts requires loading values into a set and returning its
            size. This doesn't scale when working on high-cardinality sets and/or large
            values as the required memory usage and the need to communicate those
            per-shard sets between nodes would utilize too many resources of the cluster.

            This `COUNT_DISTINCT` function is based on the
            https://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/40671.pdf[HyperLogLog++]
            algorithm, which counts based on the hashes of the values with some interesting
            properties:

            include::../../../aggregations/metrics/cardinality-aggregation.asciidoc[tag=explanation]

            The `COUNT_DISTINCT` function takes an optional second parameter to configure
            the precision threshold. The precision_threshold options allows to trade memory
            for accuracy, and defines a unique count below which counts are expected to be
            close to accurate. Above this value, counts might become a bit more fuzzy. The
            maximum supported value is 40000, thresholds above this number will have the
            same effect as a threshold of 40000. The default value is `3000`.
            """,
        isAggregation = true,
        examples = {
            @Example(file = "stats_count_distinct", tag = "count-distinct"),
            @Example(
                description = "With the optional second parameter to configure the precision threshold",
                file = "stats_count_distinct",
                tag = "count-distinct-precision"
            ),
            @Example(
                description = "The expression can use inline functions. This example splits a string into "
                    + "multiple values using the `SPLIT` function and counts the unique values",
                file = "stats_count_distinct",
                tag = "docsCountDistinctWithExpression"
            ) }
    )
    public CountDistinct(
        Source source,
        @Param(
            name = "field",
            type = { "boolean", "date", "double", "integer", "ip", "keyword", "long", "text", "version" },
            description = "Column or literal for which to count the number of distinct values."
        ) Expression field,
        @Param(
            optional = true,
            name = "precision",
            type = { "integer", "long", "unsigned_long" },
            description = "Precision threshold. Refer to <<esql-agg-count-distinct-approximate>>. "
                + "The maximum supported value is 40000. Thresholds above this number will have the "
                + "same effect as a threshold of 40000. The default value is 3000."
        ) Expression precision
    ) {
        super(source, field, precision != null ? List.of(precision) : List.of());
        this.precision = precision;
    }

    private CountDistinct(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(field());
        out.writeOptionalNamedWriteable(precision);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<CountDistinct> info() {
        return NodeInfo.create(this, CountDistinct::new, field(), precision);
    }

    @Override
    public CountDistinct replaceChildren(List<Expression> newChildren) {
        return new CountDistinct(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    public DataType dataType() {
        return DataType.LONG;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = EsqlTypeResolutions.isExact(field(), sourceText(), DEFAULT)
            .and(
                isType(
                    field(),
                    dt -> dt != DataType.UNSIGNED_LONG && dt != DataType.SOURCE,
                    sourceText(),
                    DEFAULT,
                    "any exact type except unsigned_long, _source, or counter types"
                )
            );

        if (resolution.unresolved() || precision == null) {
            return resolution;
        }
        return isWholeNumber(precision, sourceText(), SECOND).and(isFoldable(precision, sourceText(), SECOND));
    }

    @Override
    public AggregatorFunctionSupplier supplier(List<Integer> inputChannels) {
        DataType type = field().dataType();
        int precision = this.precision == null ? DEFAULT_PRECISION : ((Number) this.precision.fold()).intValue();
        if (type == DataType.BOOLEAN) {
            // Booleans ignore the precision because there are only two possible values anyway
            return new CountDistinctBooleanAggregatorFunctionSupplier(inputChannels);
        }
        if (type == DataType.DATETIME || type == DataType.LONG) {
            return new CountDistinctLongAggregatorFunctionSupplier(inputChannels, precision);
        }
        if (type == DataType.INTEGER) {
            return new CountDistinctIntAggregatorFunctionSupplier(inputChannels, precision);
        }
        if (type == DataType.DOUBLE) {
            return new CountDistinctDoubleAggregatorFunctionSupplier(inputChannels, precision);
        }
        if (type == DataType.KEYWORD || type == DataType.IP || type == DataType.VERSION || type == DataType.TEXT) {
            return new CountDistinctBytesRefAggregatorFunctionSupplier(inputChannels, precision);
        }
        throw EsqlIllegalArgumentException.illegalDataType(type);
    }

    @Override
    public Expression surrogate() {
        var s = source();
        var field = field();

        return field.foldable()
            ? new ToLong(s, new Coalesce(s, new MvCount(s, new MvDedupe(s, field)), List.of(new Literal(s, 0, DataType.INTEGER))))
            : null;
    }

    Expression precision() {
        return precision;
    }
}
