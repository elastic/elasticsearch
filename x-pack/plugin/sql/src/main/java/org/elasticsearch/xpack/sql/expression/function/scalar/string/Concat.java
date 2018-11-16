/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.string;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Expressions.ParamOrdinal;
import org.elasticsearch.xpack.sql.expression.FieldAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import static org.elasticsearch.xpack.sql.expression.function.scalar.string.ConcatFunctionProcessor.process;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

/**
 * Returns a string that is the result of concatenating the two strings received as parameters.
 * The result of the function is null only if both parameters are null, otherwise the result is the non-null
 * parameter or the concatenation of the two strings if none of them is null.
 */
public class Concat extends BinaryScalarFunction {
    
    public Concat(Location location, Expression source1, Expression source2) {
        super(location, source1, source2);
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution sourceResolution = Expressions.typeMustBeString(left(), functionName(), ParamOrdinal.FIRST);
        if (sourceResolution.unresolved()) {
            return sourceResolution;
        }

        return Expressions.typeMustBeString(right(), functionName(), ParamOrdinal.SECOND);
    }

    @Override
    protected Pipe makePipe() {
        return new ConcatFunctionPipe(location(), this, Expressions.pipe(left()), Expressions.pipe(right()));
    }
    
    @Override
    public boolean nullable() {
        return left().nullable() && right().nullable();
    }

    @Override
    public boolean foldable() {
        return left().foldable() && right().foldable();
    }

    @Override
    public Object fold() {
        return process(left().fold(), right().fold());
    }
    
    @Override
    protected Concat replaceChildren(Expression newLeft, Expression newRight) {
        return new Concat(location(), newLeft, newRight);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Concat::new, left(), right());
    }

    @Override
    public ScriptTemplate scriptWithField(FieldAttribute field) {
        return new ScriptTemplate(processScript("doc[{}].value"),
                paramsBuilder().variable(field.isInexact() ? field.exactAttribute().name() : field.name()).build(),
                dataType());
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }
}