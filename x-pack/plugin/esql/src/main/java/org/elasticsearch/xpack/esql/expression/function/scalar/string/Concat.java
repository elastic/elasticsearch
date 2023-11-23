/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

/**
 * Join strings.
 */
public class Concat extends ScalarFunction implements EvaluatorMapper {

    static final long MAX_CONCAT_LENGTH = MB.toBytes(1);

    public Concat(Source source, Expression first, List<? extends Expression> rest) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    @Override
    public DataType dataType() {
        return DataTypes.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = TypeResolution.TYPE_RESOLVED;
        for (Expression value : children()) {
            resolution = isString(value, sourceText(), DEFAULT);

            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return resolution;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var values = children().stream().map(toEvaluator).toArray(ExpressionEvaluator.Factory[]::new);
        return new ConcatEvaluator.Factory(context -> new BreakingBytesRefBuilder(context.breaker(), "concat"), values);
    }

    @Evaluator
    static BytesRef process(@Fixed(includeInToString = false, build = true) BreakingBytesRefBuilder scratch, BytesRef[] values) {
        scratch.grow(checkedTotalLength(values));
        scratch.clear();
        for (int i = 0; i < values.length; i++) {
            scratch.append(values[i]);
        }
        return scratch.bytesRefView();
    }

    private static int checkedTotalLength(BytesRef[] values) {
        int length = 0;
        for (var v : values) {
            length += v.length;
        }
        if (length > MAX_CONCAT_LENGTH) {
            throw new EsqlClientException("concatenating more than [" + MAX_CONCAT_LENGTH + "] bytes is not supported");
        }
        return length;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Concat(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Concat::new, children().get(0), children().subList(1, children().size()));
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }
}
