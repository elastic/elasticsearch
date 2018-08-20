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
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

public class ScalarFunctionAttribute extends FunctionAttribute {

    private final ScriptTemplate script;
    private final Expression orderBy;
    private final ProcessorDefinition processorDef;

    ScalarFunctionAttribute(Location location, String name, DataType dataType, ExpressionId id,
            String functionId, ScriptTemplate script, Expression orderBy, ProcessorDefinition processorDef) {
        this(location, name, dataType, null, true, id, false, functionId, script, orderBy, processorDef);
    }

    public ScalarFunctionAttribute(Location location, String name, DataType dataType, String qualifier,
            boolean nullable, ExpressionId id, boolean synthetic, String functionId, ScriptTemplate script,
            Expression orderBy, ProcessorDefinition processorDef) {
        super(location, name, dataType, qualifier, nullable, id, synthetic, functionId);

        this.script = script;
        this.orderBy = orderBy;
        this.processorDef = processorDef;
    }

    @Override
    protected NodeInfo<ScalarFunctionAttribute> info() {
        return NodeInfo.create(this, ScalarFunctionAttribute::new,
            name(), dataType(), qualifier(), nullable(), id(), synthetic(),
            functionId(), script, orderBy, processorDef);
    }

    public ScriptTemplate script() {
        return script;
    }

    public Expression orderBy() {
        return orderBy;
    }

    public ProcessorDefinition processorDef() {
        return processorDef;
    }

    @Override
    protected Expression canonicalize() {
        return new ScalarFunctionAttribute(location(), "<none>", dataType(), null, true, id(), false,
                functionId(), script, orderBy, processorDef);
    }

    @Override
    protected Attribute clone(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        return new ScalarFunctionAttribute(location, name, dataType(), qualifier, nullable, id, synthetic,
                functionId(), script, orderBy, processorDef);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), orderBy);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(orderBy, ((ScalarFunctionAttribute) obj).orderBy());
    }

    @Override
    protected String label() {
        return "s->" + functionId();
    }
}
