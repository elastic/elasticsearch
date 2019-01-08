/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.grouping;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.FunctionAttribute;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

public class GroupingFunctionAttribute extends FunctionAttribute {

    GroupingFunctionAttribute(Source source, String name, DataType dataType, ExpressionId id, String functionId) {
        this(source, name, dataType, null, Nullability.FALSE, id, false, functionId);
    }

    public GroupingFunctionAttribute(Source source, String name, DataType dataType, String qualifier,
                                     Nullability nullability, ExpressionId id, boolean synthetic, String functionId) {
        super(source, name, dataType, qualifier, nullability, id, synthetic, functionId);
    }

    @Override
    protected NodeInfo<GroupingFunctionAttribute> info() {
        return NodeInfo.create(this, GroupingFunctionAttribute::new,
                name(), dataType(), qualifier(), nullable(), id(), synthetic(), functionId());
    }

    @Override
    protected Expression canonicalize() {
        return new GroupingFunctionAttribute(source(), "<none>", dataType(), null, Nullability.TRUE, id(), false, "<none>");
    }

    @Override
    protected Attribute clone(Source source, String name, String qualifier, Nullability nullability,
                              ExpressionId id, boolean synthetic) {
        // this is highly correlated with QueryFolder$FoldAggregate#addFunction (regarding the function name within the querydsl)
        // that is the functionId is actually derived from the expression id to easily track it across contexts
        return new GroupingFunctionAttribute(source, name, dataType(), qualifier, nullability, id, synthetic, functionId());
    }

    public GroupingFunctionAttribute withFunctionId(String functionId, String propertyPath) {
        return new GroupingFunctionAttribute(source(), name(), dataType(), qualifier(), nullable(),
                id(), synthetic(), functionId);
    }

    @Override
    protected String label() {
        return "g->" + functionId();
    }
}
