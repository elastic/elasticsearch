/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.MvMax;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * Returns the maximum value of multiple columns.
 */
public class Greatest extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Greatest", Greatest::new);

    private DataType dataType;

    @FunctionInfo(
        returnType = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "version" },
        description = "Returns the maximum value from multiple columns. This is similar to <<esql-mv_max>>\n"
            + "except it is intended to run on multiple columns at once.",
        note = "When run on `keyword` or `text` fields, this returns the last string in alphabetical order. "
            + "When run on `boolean` columns this will return `true` if any values are `true`.",
        examples = @Example(file = "math", tag = "greatest")
    )
    public Greatest(
        Source source,
        @Param(
            name = "first",
            type = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "version" },
            description = "First of the columns to evaluate."
        ) Expression first,
        @Param(
            name = "rest",
            type = { "boolean", "date", "date_nanos", "double", "integer", "ip", "keyword", "long", "text", "version" },
            description = "The rest of the columns to evaluate.",
            optional = true
        ) List<Expression> rest
    ) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    private Greatest(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(children().get(0));
        out.writeNamedWriteableCollection(children().subList(1, children().size()));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        if (dataType == null) {
            resolveType();
        }
        return dataType;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        for (int position = 0; position < children().size(); position++) {
            Expression child = children().get(position);
            if (dataType == null || dataType == NULL) {
                dataType = child.dataType();
                continue;
            }
            TypeResolution resolution = TypeResolutions.isType(
                child,
                t -> t == dataType,
                sourceText(),
                TypeResolutions.ParamOrdinal.fromIndex(position),
                dataType.typeName()
            );
            if (resolution.unresolved()) {
                return resolution;
            }
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Greatest(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Greatest::new, children().get(0), children().subList(1, children().size()));
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        // force datatype initialization
        var dataType = dataType();
        ExpressionEvaluator.Factory[] factories = children().stream()
            .map(e -> toEvaluator.apply(new MvMax(e.source(), e)))
            .toArray(ExpressionEvaluator.Factory[]::new);
        if (dataType == DataType.BOOLEAN) {
            return new GreatestBooleanEvaluator.Factory(source(), factories);
        }
        if (dataType == DataType.DOUBLE) {
            return new GreatestDoubleEvaluator.Factory(source(), factories);
        }
        if (dataType == DataType.INTEGER) {
            return new GreatestIntEvaluator.Factory(source(), factories);
        }
        if (dataType == DataType.LONG || dataType == DataType.DATETIME || dataType == DataType.DATE_NANOS) {
            return new GreatestLongEvaluator.Factory(source(), factories);
        }
        if (DataType.isString(dataType) || dataType == DataType.IP || dataType == DataType.VERSION || dataType == DataType.UNSUPPORTED) {

            return new GreatestBytesRefEvaluator.Factory(source(), factories);
        }
        throw EsqlIllegalArgumentException.illegalDataType(dataType);
    }

    @Evaluator(extraName = "Boolean")
    static boolean process(boolean[] values) {
        for (boolean v : values) {
            if (v) {
                return true;
            }
        }
        return false;
    }

    @Evaluator(extraName = "BytesRef")
    static BytesRef process(BytesRef[] values) {
        BytesRef max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = max.compareTo(values[i]) > 0 ? max : values[i];
        }
        return max;
    }

    @Evaluator(extraName = "Int")
    static int process(int[] values) {
        int max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    @Evaluator(extraName = "Long")
    static long process(long[] values) {
        long max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    @Evaluator(extraName = "Double")
    static double process(double[] values) {
        double max = values[0];
        for (int i = 1; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        return max;
    }

    // TODO unsigned long
}
