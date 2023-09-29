/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.TriFunction;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.expression.function.Named;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.versionfield.Version;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;

public class ToVersion extends AbstractConvertFunction {

    private static final Map<
        DataType,
        TriFunction<EvalOperator.ExpressionEvaluator, Source, DriverContext, EvalOperator.ExpressionEvaluator>> EVALUATORS = Map.ofEntries(
            Map.entry(VERSION, (fieldEval, source, driverContext) -> fieldEval),
            Map.entry(KEYWORD, ToVersionFromStringEvaluator::new),
            Map.entry(TEXT, ToVersionFromStringEvaluator::new)
        );

    public ToVersion(Source source, @Named("v") Expression v) {
        super(source, v);
    }

    @Override
    protected
        Map<DataType, TriFunction<EvalOperator.ExpressionEvaluator, Source, DriverContext, EvalOperator.ExpressionEvaluator>>
        evaluators() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return VERSION;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToVersion(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToVersion::new, field());
    }

    @ConvertEvaluator(extraName = "FromString")
    static BytesRef fromKeyword(BytesRef asString) {
        return new Version(asString.utf8ToString()).toBytesRef();
    }
}
