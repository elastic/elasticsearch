/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlConfigurationFunction;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class ToUpper extends EsqlConfigurationFunction {

    private final Expression field;

    @FunctionInfo(
        returnType = { "keyword", "text" },
        description = "Returns a new string representing the input string converted to upper case."
    )
    public ToUpper(
        Source source,
        @Param(name = "str", type = { "keyword", "text" }, description = "The input string") Expression field,
        Configuration configuration
    ) {
        super(source, List.of(field), configuration);
        this.field = field;
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
        return BytesRefs.toBytesRef(val.utf8ToString().toUpperCase(locale));
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var fieldEvaluator = toEvaluator.apply(field);
        return new ToUpperEvaluator.Factory(source(), fieldEvaluator, ((EsqlConfiguration) configuration()).locale());
    }

    public Expression field() {
        return field;
    }

    public ToUpper replaceChild(Expression child) {
        return new ToUpper(source(), child, configuration());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        assert newChildren.size() == 1;
        return replaceChild(newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToUpper::new, field, configuration());
    }
}
