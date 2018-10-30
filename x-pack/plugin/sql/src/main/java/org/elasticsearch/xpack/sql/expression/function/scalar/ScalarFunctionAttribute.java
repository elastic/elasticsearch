/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.function.FunctionAttribute;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

public class ScalarFunctionAttribute extends FunctionAttribute {

    private final ScriptTemplate script;
    private final Expression orderBy;
    private final Pipe pipe;

    ScalarFunctionAttribute(Location location, String name, DataType dataType, ExpressionId id,
            String functionId, ScriptTemplate script, Expression orderBy, Pipe processorDef) {
        this(location, name, dataType, null, true, id, false, functionId, script, orderBy, processorDef);
    }

    public ScalarFunctionAttribute(Location location, String name, DataType dataType, String qualifier,
            boolean nullable, ExpressionId id, boolean synthetic, String functionId, ScriptTemplate script,
            Expression orderBy, Pipe pipe) {
        super(location, name, dataType, qualifier, nullable, id, synthetic, functionId);

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
        return new ScalarFunctionAttribute(location(), "<none>", dataType(), null, true, id(), false,
                functionId(), script, orderBy, pipe);
    }

    @Override
    protected Attribute clone(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        return new ScalarFunctionAttribute(location, name, dataType(), qualifier, nullable, id, synthetic,
                functionId(), script, orderBy, pipe);
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