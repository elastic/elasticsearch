/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.FunctionAttribute;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Objects;

public class AggregateFunctionAttribute extends FunctionAttribute {

    private final String propertyPath;

    AggregateFunctionAttribute(Source source, String name, DataType dataType, ExpressionId id,
            String functionId, String propertyPath) {
        this(source, name, dataType, null, Nullability.FALSE, id, false, functionId, propertyPath);
    }

    public AggregateFunctionAttribute(Source source, String name, DataType dataType, String qualifier,
                                      Nullability nullability, ExpressionId id, boolean synthetic, String functionId, String propertyPath) {
        super(source, name, dataType, qualifier, nullability, id, synthetic, functionId);
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
        return new AggregateFunctionAttribute(source(), "<none>", dataType(), null, Nullability.TRUE, id(), false, "<none>", null);
    }

    @Override
    protected Attribute clone(Source source, String name, String qualifier, Nullability nullability, ExpressionId id, boolean synthetic) {
        // this is highly correlated with QueryFolder$FoldAggregate#addFunction (regarding the function name within the querydsl)
        // that is the functionId is actually derived from the expression id to easily track it across contexts
        return new AggregateFunctionAttribute(source, name, dataType(), qualifier, nullability, id, synthetic, functionId(), propertyPath);
    }

    public AggregateFunctionAttribute withFunctionId(String functionId, String propertyPath) {
        return new AggregateFunctionAttribute(source(), name(), dataType(), qualifier(), nullable(),
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
