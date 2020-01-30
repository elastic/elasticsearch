/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.MathProcessor.MathOperation;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public abstract class MathFunction extends UnaryScalarFunction {

    protected MathFunction(Source source) {
        super(source);
    }

    protected MathFunction(Source source, Expression field) {
        super(source, field);
    }

    @Override
    public boolean foldable() {
        return field().foldable();
    }

    @Override
    public Object fold() {
        return operation().apply(field().fold());
    }

    @Override
    public String processScript(String template) {
        return super.processScript(format(
            Locale.ROOT, "{sql}.%s(%s)", getClass().getSimpleName().toLowerCase(Locale.ROOT), template));
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }
    
    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        return isNumeric(field(), sourceText(), ParamOrdinal.DEFAULT);
    }

    @Override
    protected Processor makeProcessor() {
        return new MathProcessor(operation());
    }

    protected abstract MathOperation operation();

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        MathFunction other = (MathFunction) obj;
        return Objects.equals(other.field(), field());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field());
    }
}
