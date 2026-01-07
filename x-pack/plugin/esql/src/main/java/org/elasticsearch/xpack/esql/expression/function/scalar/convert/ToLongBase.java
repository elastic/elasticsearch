/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToLong;

//
public class ToLongBase extends EsqlScalarFunction {

    private static final TransportVersion ESQL_BASE_CONVERSION = TransportVersion.fromName("esql_base_conversion");

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToLongBase",
        ToLongBase::new
    );

    private final Expression string;
    private final Expression base;

    public ToLongBase(Source source, Expression string, Expression base) {
        super(source, List.of(string, base));
        this.string = string;
        this.base = base;
    }

    private ToLongBase(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_BASE_CONVERSION) == false) {
            throw new UnsupportedOperationException("version does not support to_long(string, base)");
        }
        source().writeTo(out);
        out.writeNamedWriteable(string);
        out.writeNamedWriteable(base);
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
        TypeResolution resolution = isString(string, sourceText(), FIRST);
        if (resolution.resolved()) {
            resolution = isType(base, dt -> dt == INTEGER, sourceText(), SECOND, "integer");
        }
        return resolution;
    }

    @Override
    public boolean foldable() {
        return string.foldable() && base.foldable();
    }

    @Evaluator(warnExceptions = { InvalidArgumentException.class })
    static long process(BytesRef string, int base) {
        var value = string.utf8ToString();
        try {
            if (base == 10) {
                return stringToLong(value);
            }
            if (base == 16 && value.startsWith("0x")) {
                value = value.substring(2);
            }
            return Long.parseLong(value, base);

        } catch (NumberFormatException e) {
            throw new InvalidArgumentException(e, "Unable to convert [{}] to number of base [{}]", value, base);
        }
    }

    @Override
    public final Expression replaceChildren(List<Expression> newChildren) {
        return new ToLongBase(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToLongBase::new, string, base);
    }

    public Expression string() {
        return string;
    }

    public Expression base() {
        return base;
    }

    @Override
    public DataType dataType() {
        return LONG;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var stringEval = toEvaluator.apply(string);
        var baseEval = toEvaluator.apply(base);
        return new ToLongBaseEvaluator.Factory(source(), stringEval, baseEval);
    }
}
