/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.arrayType;

/**
 * Marker function to return all the values of one multi-value field.
 * Only allowed in projections.
 */
public class Array extends Function {

    private final Expression field;

    public Array(Source source, Expression field) {
        super(source, singletonList(field));
        this.field = field;
    }

    public Expression field() {
        return field;
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    @Override
    public DataType dataType() {
        return arrayType(field.dataType());
    }

    @Override
    protected TypeResolution resolveType() {
        return arrayType(field.dataType()) != null
            ? TypeResolution.TYPE_RESOLVED
            : new TypeResolution(format("Cannot use field [{}] of type [{}] as an array", field.source().text(), field.dataType()));
    }

    @Override
    public ScriptTemplate asScript() {
        throw new SqlIllegalArgumentException("ARRAY cannot be scripted");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Array(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Array::new, field());
    }
}
