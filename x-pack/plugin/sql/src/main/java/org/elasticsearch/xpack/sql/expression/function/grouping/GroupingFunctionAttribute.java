/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.grouping;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.ExpressionId;
import org.elasticsearch.xpack.sql.expression.function.FunctionAttribute;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

public class GroupingFunctionAttribute extends FunctionAttribute {

    GroupingFunctionAttribute(Location location, String name, DataType dataType, ExpressionId id, String functionId) {
        this(location, name, dataType, null, false, id, false, functionId);
    }

    public GroupingFunctionAttribute(Location location, String name, DataType dataType, String qualifier,
            boolean nullable, ExpressionId id, boolean synthetic, String functionId) {
        super(location, name, dataType, qualifier, nullable, id, synthetic, functionId);
    }

    @Override
    protected NodeInfo<GroupingFunctionAttribute> info() {
        return NodeInfo.create(this, GroupingFunctionAttribute::new,
                name(), dataType(), qualifier(), nullable(), id(), synthetic(), functionId());
    }

    @Override
    protected Expression canonicalize() {
        return new GroupingFunctionAttribute(location(), "<none>", dataType(), null, true, id(), false, "<none>");
    }

    @Override
    protected Attribute clone(Location location, String name, String qualifier, boolean nullable, ExpressionId id, boolean synthetic) {
        // this is highly correlated with QueryFolder$FoldAggregate#addFunction (regarding the function name within the querydsl)
        // that is the functionId is actually derived from the expression id to easily track it across contexts
        return new GroupingFunctionAttribute(location, name, dataType(), qualifier, nullable, id, synthetic, functionId());
    }

    public GroupingFunctionAttribute withFunctionId(String functionId, String propertyPath) {
        return new GroupingFunctionAttribute(location(), name(), dataType(), qualifier(), nullable(),
                id(), synthetic(), functionId);
    }

    @Override
    protected String label() {
        return "g->" + functionId();
    }
}
