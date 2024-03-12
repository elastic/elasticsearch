/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.capabilities.Validatable;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.InvalidArgumentException;
import org.elasticsearch.xpack.ql.common.Failures;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.expression.Validations.isFoldable;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

/**
 * Returns the index for the first value in a multivalued field that matches a regular expression.
 */
public class MvFind extends ScalarFunction implements EvaluatorMapper, Validatable {
    private final Expression field, pattern;

    @FunctionInfo(
        returnType = { "integer" },
        description = "Returns the index for the first value in a multivalued field that matches a regular expression."
    )
    public MvFind(
        Source source,
        @Param(name = "field", type = { "keyword", "text" }, description = "A multivalued field") Expression field,
        @Param(name = "pattern", type = { "keyword", "text" }, description = "A regular expression") Expression pattern
    ) {
        super(source, Arrays.asList(field, pattern));
        this.field = field;
        this.pattern = pattern;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isString(field, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        return isString(pattern, sourceText(), SECOND);
    }

    @Override
    public boolean foldable() {
        return field.foldable() && pattern.foldable();
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(
        Function<Expression, EvalOperator.ExpressionEvaluator.Factory> toEvaluator
    ) {
        if (pattern.foldable()) {
            try {
                Pattern p = Pattern.compile(((BytesRef) pattern.fold()).utf8ToString());
                return new MvFindEvaluator.Factory(source(), toEvaluator.apply(field), p);
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException(e, "invalid regular expression for [{}]: {}", sourceText(), e.getMessage());
            }
        }
        return EvalOperator.CONSTANT_NULL_FACTORY;
    }

    @Override
    public Object fold() {
        return EvaluatorMapper.super.fold();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MvFind(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MvFind::new, field, pattern);
    }

    @Override
    public DataType dataType() {
        return DataTypes.INTEGER;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, pattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MvFind other = (MvFind) obj;
        return Objects.equals(other.field, field) && Objects.equals(other.pattern, pattern);
    }

    @Evaluator
    static void process(IntBlock.Builder builder, int position, BytesRefBlock field, @Fixed Pattern pattern) {
        int fieldValueCount = field.getValueCount(position);
        int first = field.getFirstValueIndex(position);
        boolean foundMatch = false;
        BytesRef fieldScratch = new BytesRef();
        int i;
        for (i = 0; i < fieldValueCount; i++) {
            Matcher matcher = pattern.matcher(field.getBytesRef(i + first, fieldScratch).utf8ToString());
            if (matcher.find()) {
                foundMatch = true;
                break;
            }
        }
        if (foundMatch) {
            builder.appendInt(i);
        } else {
            builder.appendNull();
        }
    }

    @Override
    public void validate(Failures failures) {
        String operation = sourceText();
        failures.add(isFoldable(pattern, operation, SECOND));
    }
}
