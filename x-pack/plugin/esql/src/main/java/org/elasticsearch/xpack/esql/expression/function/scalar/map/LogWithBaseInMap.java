/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.map;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Cast;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;

public class LogWithBaseInMap extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "LogWithBaseInMap",
        LogWithBaseInMap::new
    );

    private final Expression number;

    private final Expression map;

    private static final String BASE = "base";

    @FunctionInfo(
        returnType = "double",
        description = "Returns the logarithm of a value to a base. The input can be any numeric value, "
            + "the return value is always a double.\n"
            + "\n"
            + "Logs of zero, negative numbers, and base of one return `null` as well as a warning."
    )
    public LogWithBaseInMap(
        Source source,
        @Param(
            name = "number",
            type = { "double", "integer", "long" },
            description = "Numeric expression. If `null`, the function returns `null`."
        ) Expression number,
        @MapParam(
            params = { @MapParam.MapParamEntry(name = "base", valueHint = { "2", "2.0" }) },
            description = "Input value. The input is a valid constant map expression.",
            optional = true
        ) Expression option
    ) {
        super(source, option == null ? Collections.singletonList(number) : List.of(number, option));
        this.number = number;
        this.map = option;
    }

    private LogWithBaseInMap(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(number);
        out.writeOptionalNamedWriteable(map);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        // validate field type
        TypeResolution resolution = isNumeric(number, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (map != null) {
            // MapExpression does not have a DataType associated with it
            resolution = isMapExpression(map, sourceText(), SECOND);
            if (resolution.unresolved()) {
                return resolution;
            }
            return validateOptions();
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return DOUBLE;
    }

    @Override
    public boolean foldable() {
        return number.foldable();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new LogWithBaseInMap(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, LogWithBaseInMap::new, number, map);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var valueEval = Cast.cast(source(), number.dataType(), DataType.DOUBLE, toEvaluator.apply(number));
        double base = Math.E;
        if (map instanceof MapExpression me) {
            Expression b = me.get(BASE);
            if (b != null && b.foldable()) {
                Object v = b.fold(toEvaluator.foldCtx());
                if (v instanceof BytesRef br) {
                    v = br.utf8ToString();
                }
                base = Double.parseDouble(v.toString());
            }
        }
        return new LogWithBaseInMapEvaluator.Factory(source(), valueEval, base);
    }

    @Evaluator(warnExceptions = { ArithmeticException.class })
    static double process(double value, @Fixed double base) throws ArithmeticException {
        if (base <= 0d || value <= 0d) {
            throw new ArithmeticException("Log of non-positive number");
        }
        if (base == 1d) {
            throw new ArithmeticException("Log of base 1");
        }
        return Math.log10(value) / Math.log10(base);
    }

    public Expression number() {
        return number;
    }

    public Expression base() {
        return map;
    }

    private TypeResolution validateOptions() {
        for (EntryExpression entry : ((MapExpression) map).entryExpressions()) {
            Expression key = entry.key();
            Expression value = entry.value();
            TypeResolution resolution = isFoldable(key, sourceText(), SECOND).and(isFoldable(value, sourceText(), SECOND));
            if (resolution.unresolved()) {
                return resolution;
            }
            Object k = key instanceof Literal l ? l.value() : null;
            Object v = value instanceof Literal l ? l.value() : null;
            if (k == null) {
                return new TypeResolution(
                    format(null, "Invalid option key in [{}], expected a literal value but got [{}]", sourceText(), key.sourceText())
                );
            }

            if (v == null) {
                return new TypeResolution(
                    format(null, "Invalid option value in [{}], expected a constant value but got [{}]", sourceText(), value.sourceText())
                );
            }
            String base = k instanceof BytesRef br ? br.utf8ToString() : k.toString();
            String number = v instanceof BytesRef br ? br.utf8ToString() : v.toString();
            // validate the key is in SUPPORTED_OPTIONS
            if (base.equals(BASE) == false) {
                return new TypeResolution(
                    format(null, "Invalid option key in [{}], expected base but got [{}]", sourceText(), key.sourceText())
                );
            }
            // validate the value is valid for the key provided
            try {
                Double.parseDouble(number);
            } catch (NumberFormatException e) {
                return new TypeResolution(
                    format(null, "Invalid option value in [{}], expected a numeric number but got [{}]", sourceText(), v)
                );
            }

        }
        return TypeResolution.TYPE_RESOLVED;
    }
}
