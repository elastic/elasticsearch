/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.type.DataTypes.NULL;

public class Case extends ScalarFunction implements Mappable {

    private DataType dataType;

    public Case(Source source, List<Expression> fields) {
        super(source, fields);
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

        if (children().size() < 2) {
            return new TypeResolution(format(null, "expected at least two arguments in [{}] but got {}", sourceText(), children().size()));
        }

        for (int i = 0; i + 1 < children().size(); i += 2) {
            Expression condition = children().get(i);
            TypeResolution resolution = TypeResolutions.isBoolean(condition, sourceText(), TypeResolutions.ParamOrdinal.fromIndex(i));
            if (resolution.unresolved()) {
                return resolution;
            }

            resolution = resolveValueTypeAt(i + 1);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        if (children().size() % 2 == 1) { // check default value
            return resolveValueTypeAt(children().size() - 1);
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    private TypeResolution resolveValueTypeAt(int index) {
        Expression value = children().get(index);
        if (dataType == null || dataType == NULL) {
            dataType = value.dataType();
        } else {
            return TypeResolutions.isType(
                value,
                t -> t == dataType,
                sourceText(),
                TypeResolutions.ParamOrdinal.fromIndex(index),
                dataType.typeName()
            );
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public Nullability nullable() {
        return Nullability.UNKNOWN;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Case(source(), newChildren);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Case::new, children());
    }

    @Override
    public boolean foldable() {
        for (int c = 0; c + 1 < children().size(); c += 2) {
            Expression child = children().get(c);
            if (child.foldable() == false) {
                return false;
            }
            Boolean b = (Boolean) child.fold();
            if (b != null && b) {
                return children().get(c + 1).foldable();
            }
        }
        if (children().size() % 2 == 0) {
            return true;
        }
        return children().get(children().size() - 1).foldable();
    }

    @Override
    public Object fold() {
        for (int c = 0; c + 1 < children().size(); c += 2) {
            Expression child = children().get(c);
            Boolean b = (Boolean) child.fold();
            if (b != null && b) {
                return children().get(c + 1).fold();
            }
        }
        if (children().size() % 2 == 0) {
            return null;
        }
        return children().get(children().size() - 1).fold();
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        return () -> new CaseEvaluator(children().stream().map(toEvaluator).map(Supplier::get).toList());
    }

    private record CaseEvaluator(List<EvalOperator.ExpressionEvaluator> children) implements EvalOperator.ExpressionEvaluator {
        @Override
        public Object computeRow(Page page, int position) {
            for (int i = 0; i + 1 < children().size(); i += 2) {
                EvalOperator.ExpressionEvaluator child = children.get(i);
                Boolean condition = (Boolean) child.computeRow(page, position);
                if (condition != null && condition) {
                    return children.get(i + 1).computeRow(page, position);
                }
            }
            // return default, if one provided, or null otherwise
            return children().size() % 2 == 0 ? null : children.get(children().size() - 1).computeRow(page, position);
        }
    }
}
