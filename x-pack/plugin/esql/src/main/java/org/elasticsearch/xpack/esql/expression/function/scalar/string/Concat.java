/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Join strings.
 */
public class Concat extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Concat", Concat::new);

    static final long MAX_CONCAT_LENGTH = MB.toBytes(1);

    @FunctionInfo(
        returnType = "keyword",
        description = "Concatenates two or more strings.",
        examples = @Example(file = "eval", tag = "docsConcat")
    )
    public Concat(
        Source source,
        @Param(name = "string1", type = { "keyword", "text" }, description = "Strings to concatenate.") Expression first,
        @Param(name = "string2", type = { "keyword", "text" }, description = "Strings to concatenate.") List<? extends Expression> rest
    ) {
        super(source, Stream.concat(Stream.of(first), rest.stream()).toList());
    }

    private Concat(StreamInput in) throws IOException {
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
        return DataType.KEYWORD;
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
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var values = children().stream().map(toEvaluator::apply).toArray(ExpressionEvaluator.Factory[]::new);
        return new ConcatEvaluator.Factory(source(), context -> new BreakingBytesRefBuilder(context.breaker(), "concat"), values);
    }

    @Evaluator
    static BytesRef process(@Fixed(includeInToString = false, scope = THREAD_LOCAL) BreakingBytesRefBuilder scratch, BytesRef[] values) {
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
}
