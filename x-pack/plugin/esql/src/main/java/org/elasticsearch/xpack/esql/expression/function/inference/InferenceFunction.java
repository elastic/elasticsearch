/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class InferenceFunction extends Function {

    private Expression inferenceId;

    private final Expression options;

    @SuppressWarnings("this-escape")
    protected InferenceFunction(Source source, List<Expression> children, Expression options) {
        super(source, Stream.concat(children.stream(), Stream.of(options)).toList());
        this.inferenceId = parseInferenceId(options);
        this.options = options;
    }

    public Expression inferenceId() {
        return inferenceId;
    }

    public Expression options() {
        return options;
    }

    protected abstract Expression parseInferenceId(Expression options);

    public abstract List<Attribute> temporaryAttributes();

    protected Expression readOption(String optionName,  TypeResolutions.ParamOrdinal optionParamOrd, Expression options) {
        return readOption(optionName, optionParamOrd, options, Literal.NULL);
    }

    protected Expression readOption(String optionName,  TypeResolutions.ParamOrdinal optionParamOrd, Expression options, Expression defaultValue) {
        if (options != null && options.dataType() != DataType.NULL &&  options instanceof MapExpression mapOptions) {
            return mapOptions.getOrDefault(optionName, defaultValue);
        }

        return defaultValue;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return resolveOptions().and(resolveParams());
    }

    protected abstract TypeResolution resolveParams();

    protected abstract TypeResolution resolveOptions();

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        InferenceFunction that = (InferenceFunction) o;
        return Objects.equals(inferenceId, that.inferenceId) && Objects.equals(options, that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), inferenceId, options);
    }
}
