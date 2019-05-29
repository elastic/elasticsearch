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

    // used when dealing with a inner agg (avg -> stats) to keep track of
    // packed id
    // used since the functionId points to the compoundAgg
    private final ExpressionId innerId;
    private final String propertyPath;

    AggregateFunctionAttribute(Source source, String name, DataType dataType, ExpressionId id, String functionId) {
        this(source, name, dataType, null, Nullability.FALSE, id, false, functionId, null, null);
    }

    AggregateFunctionAttribute(Source source, String name, DataType dataType, ExpressionId id, String functionId, ExpressionId innerId,
            String propertyPath) {
        this(source, name, dataType, null, Nullability.FALSE, id, false, functionId, innerId, propertyPath);
    }

    public AggregateFunctionAttribute(Source source, String name, DataType dataType, String qualifier, Nullability nullability,
            ExpressionId id, boolean synthetic, String functionId, ExpressionId innerId, String propertyPath) {
        super(source, name, dataType, qualifier, nullability, id, synthetic, functionId);
        this.innerId = innerId;
        this.propertyPath = propertyPath;
    }

    @Override
    protected NodeInfo<AggregateFunctionAttribute> info() {
        return NodeInfo.create(this, AggregateFunctionAttribute::new, name(), dataType(), qualifier(), nullable(), id(), synthetic(),
                functionId(), innerId, propertyPath);
    }

    public ExpressionId innerId() {
        return innerId != null ? innerId : id();
    }

    public String propertyPath() {
        return propertyPath;
    }

    @Override
    protected Expression canonicalize() {
        return new AggregateFunctionAttribute(source(), "<none>", dataType(), null, Nullability.TRUE, id(), false, "<none>", null, null);
    }

    @Override
    protected Attribute clone(Source source, String name, String qualifier, Nullability nullability, ExpressionId id, boolean synthetic) {
        // this is highly correlated with QueryFolder$FoldAggregate#addFunction (regarding the function name within the querydsl)
        // that is the functionId is actually derived from the expression id to easily track it across contexts
        return new AggregateFunctionAttribute(source, name, dataType(), qualifier, nullability, id, synthetic, functionId(), innerId,
                propertyPath);
    }

    public AggregateFunctionAttribute withFunctionId(String functionId, String propertyPath) {
        return new AggregateFunctionAttribute(source(), name(), dataType(), qualifier(), nullable(), id(), synthetic(), functionId, innerId,
                propertyPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), innerId, propertyPath);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            AggregateFunctionAttribute other = (AggregateFunctionAttribute) obj;
            return Objects.equals(innerId, other.innerId) && Objects.equals(propertyPath, other.propertyPath);
        }
        return false;
    }

    @Override
    protected String label() {
        return "a->" + innerId();
    }
}