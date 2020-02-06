/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.expression.function.scalar.string.StringProcessor.StringOperation;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isInteger;
import static org.elasticsearch.xpack.ql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Base unary function for text manipulating SQL functions that receive as parameter a number
 */
public abstract class UnaryStringIntFunction extends UnaryScalarFunction {

    protected UnaryStringIntFunction(Source source, Expression field) {
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
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }
        return isInteger(field(), sourceText(), ParamOrdinal.DEFAULT);
    }

    @Override
    protected Processor makeProcessor() {
        return new StringProcessor(operation());
    }

    protected abstract StringOperation operation();
    
    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript("doc[{}].value"),
                paramsBuilder().variable(field.name()).build(),
                dataType());
    }
    
    @Override
    public String processScript(String template) {
        return super.processScript(
                format(Locale.ROOT, "{sql}.%s(%s)",
                        operation().toString().toLowerCase(Locale.ROOT),
                        template));
    }

    @Override
    public int hashCode() {
        return Objects.hash(field());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        UnaryStringIntFunction other = (UnaryStringIntFunction) obj;
        return Objects.equals(other.field(), field());
    }
}
