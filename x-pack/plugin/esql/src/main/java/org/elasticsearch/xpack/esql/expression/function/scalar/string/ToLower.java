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
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

public class ToLower extends EsqlConfigurationFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToLower", ToLower::new);

    private final Expression field;

    @FunctionInfo(
        returnType = { "keyword", "text" },
        description = "Returns a new string representing the input string converted to lower case.",
        examples = @Example(file = "string", tag = "to_lower")
    )
    public ToLower(
        Source source,
        @Param(
            name = "str",
            type = { "keyword", "text" },
            description = "String expression. If `null`, the function returns `null`."
        ) Expression field,
        Configuration configuration
    ) {
        super(source, List.of(field), configuration);
        this.field = field;
    }

    private ToLower(StreamInput in) throws IOException {
        this(Source.EMPTY, in.readNamedWriteable(Expression.class), ((PlanStreamInput) in).configuration());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(field());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isString(field, sourceText(), DEFAULT);
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Evaluator
    static BytesRef process(BytesRef val, @Fixed Locale locale) {
        return BytesRefs.toBytesRef(val.utf8ToString().toLowerCase(locale));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        return new ToLowerEvaluator.Factory(source(), fieldEvaluator, configuration().locale());
    }

    public Expression field() {
        return field;
    }

    public ToLower replaceChild(Expression child) {
        return new ToLower(source(), child, configuration());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return replaceChild(newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToLower::new, field, configuration());
    }
}
