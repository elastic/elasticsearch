/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.function.FunctionAttribute;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

public class AggregateFunctionAttribute extends FunctionAttribute {

    private final String propertyPath;

    AggregateFunctionAttribute(Location location, String name, DataType dataType, ExpressionId id,
            String functionId, String propertyPath) {
        this(location, name, dataType, null, false, id, false, functionId, propertyPath);
    }

    public AggregateFunctionAttribute(Location location, String name, DataType dataType, String qualifier,
            boolean nullable, ExpressionId id, boolean synthetic, String functionId, String propertyPath) {
        super(location, name, dataType, qualifier, nullable, id, synthetic, functionId);
        this.propertyPath = propertyPath;
    }

    @Override
    protected NodeInfo<AggregateFunctionAttribute> info() {
        return NodeInfo.create(this, AggregateFunctionAttribute::new,
            name(), dataType(), qualifier(), nullable(), id(), synthetic(), functionId(), propertyPath);
    }

    public String propertyPath() {
        return propertyPath;
    }

    @Override
    protected Expression canonicalize() {
        return new AggregateFunctionAttribute(location(), "<none>", dataType(), null, true, id(), false, "<none>", null);
    }

    @Override
    protected Attribute clone(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        // this is highly correlated with QueryFolder$FoldAggregate#addFunction (regarding the function name within the querydsl)
        // that is the functionId is actually derived from the expression id to easily track it across contexts
        return new AggregateFunctionAttribute(location, name, dataType(), qualifier, nullable, id, synthetic, functionId(), propertyPath);
    }

    public AggregateFunctionAttribute withFunctionId(String functionId, String propertyPath) {
        return new AggregateFunctionAttribute(location(), name(), dataType(), qualifier(), nullable(),
                id(), synthetic(), functionId, propertyPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), propertyPath);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(propertyPath(), ((AggregateFunctionAttribute) obj).propertyPath());
    }

    @Override
    protected String label() {
        return "a->" + functionId();
    }
}
