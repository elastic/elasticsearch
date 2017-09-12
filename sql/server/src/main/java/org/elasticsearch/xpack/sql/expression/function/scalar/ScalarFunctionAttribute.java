/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.TypedAttribute;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

public class ScalarFunctionAttribute extends TypedAttribute {

    private final ScriptTemplate script;
    private final Expression orderBy;
    private final ProcessorDefinition processorDef;

    ScalarFunctionAttribute(Location location, String name, DataType dataType, ExpressionId id, ScriptTemplate script, Expression orderBy, ProcessorDefinition processorDef) {
        this(location, name, dataType, null, true, id, false, script, orderBy, processorDef);
    }

    ScalarFunctionAttribute(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic, ScriptTemplate script, Expression orderBy, ProcessorDefinition processorDef) {
        super(location, name, dataType, qualifier, nullable, id, synthetic);
        this.script = script;
        this.orderBy = orderBy;
        this.processorDef = processorDef;
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
        return new ScalarFunctionAttribute(location(), "<none>", dataType(), null, true, id(), false, script, orderBy, processorDef);
    }

    @Override
    protected Attribute clone(Location location, String name, DataType dataType, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        return new ScalarFunctionAttribute(location, name, dataType, qualifier, nullable, id, synthetic, script, orderBy, processorDef);
    }

    @Override
    protected String label() {
        return "s";
    }
}