/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.FunctionAttribute;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

public class ScalarFunctionAttribute extends FunctionAttribute {

    private final ScriptTemplate script;
    private final Expression orderBy;
    private final Pipe pipe;

    ScalarFunctionAttribute(Source source, String name, DataType dataType, ExpressionId id,
            String functionId, ScriptTemplate script, Expression orderBy, Pipe processorDef) {
        this(source, name, dataType, null, Nullability.TRUE, id, false, functionId, script, orderBy, processorDef);
    }

    public ScalarFunctionAttribute(Source source, String name, DataType dataType, String qualifier,
                                   Nullability nullability, ExpressionId id, boolean synthetic, String functionId, ScriptTemplate script,
                                   Expression orderBy, Pipe pipe) {
        super(source, name, dataType, qualifier, nullability, id, synthetic, functionId);

        this.script = script;
        this.orderBy = orderBy;
        this.pipe = pipe;
    }

    @Override
    protected NodeInfo<ScalarFunctionAttribute> info() {
        return NodeInfo.create(this, ScalarFunctionAttribute::new,
            name(), dataType(), qualifier(), nullable(), id(), synthetic(),
            functionId(), script, orderBy, pipe);
    }

    public ScriptTemplate script() {
        return script;
    }

    public Expression orderBy() {
        return orderBy;
    }

    @Override
    public Pipe asPipe() {
        return pipe;
    }

    @Override
    protected Expression canonicalize() {
        return new ScalarFunctionAttribute(source(), "<none>", dataType(), null, Nullability.TRUE, id(), false,
                functionId(), script, orderBy, pipe);
    }

    @Override
    protected Attribute clone(Source source, String name, String qualifier, Nullability nullability,
                              ExpressionId id, boolean synthetic) {
        return new ScalarFunctionAttribute(source, name, dataType(), qualifier, nullability,
            id, synthetic, functionId(), script, orderBy, pipe);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), script(), pipe, orderBy);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            ScalarFunctionAttribute other = (ScalarFunctionAttribute) obj;
            return Objects.equals(script, other.script())
                    && Objects.equals(pipe, other.asPipe())
                    && Objects.equals(orderBy, other.orderBy());
        }
        return false;
    }

    @Override
    protected String label() {
        return "s->" + functionId();
    }
}
