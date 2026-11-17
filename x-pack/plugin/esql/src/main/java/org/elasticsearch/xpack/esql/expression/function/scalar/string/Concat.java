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
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.EsqlClientException;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.common.unit.ByteSizeUnit.MB;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Join strings.
 */
public class Concat extends EsqlScalarFunction implements AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "Concat", Concat::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(Concat.class).unaryVariadic(Concat::new).name("concat");

    /**
     * The most bytes a concatenated value may have. Building the value is tracked by the circuit breaker, but
     * whatever reads it afterwards may copy it out of its block without the breaker knowing, and chained
     * {@code CONCAT}s multiply their input, so the size of a single value stays bounded.
     */
    static final long MAX_CONCAT_LENGTH = MB.toBytes(10);

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA) },
        returnType = "keyword",
        briefSummary = "Concatenates two or more strings.",
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
        return new ConcatEvaluator.Factory(source(), values);
    }

    /**
     * Copies the values straight into the result instead of returning the concatenated value, so it never
     * has to be built in a buffer of its own first.
     */
    @Evaluator
    static void process(BytesRefBlock.Builder builder, BytesRef[] values) {
        long length = 0;
        for (BytesRef v : values) {
            length += v.length;
        }
        if (length > MAX_CONCAT_LENGTH) {
            throw new EsqlClientException("concatenating more than [" + MAX_CONCAT_LENGTH + "] bytes is not supported");
        }
        builder.appendConcatenated(values);
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
