/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.regex;

import org.elasticsearch.xpack.sql.expression.BinaryExpression;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.regex.Pattern;

public class Like extends BinaryExpression {

    public Like(Location location, Expression left, LikePattern right) {
        super(location, left, right);
    }

    @Override
    protected NodeInfo<Like> info() {
        return NodeInfo.create(this, Like::new, left(), right());
    }

    @Override
    protected BinaryExpression replaceChildren(Expression newLeft, Expression newRight) {
        return new Like(location(), newLeft, (LikePattern) newRight);
    }

    public LikePattern right() {
        return (LikePattern) super.right();
    }

    @Override
    public boolean foldable() {
        // right() is not directly foldable in any context but Like can fold it.
        return left().foldable();
    }

    @Override
    public Object fold() {
        Pattern p = Pattern.compile(right().asJavaRegex());
        return p.matcher(left().fold().toString()).matches();
    }

    @Override
    public Like swapLeftAndRight() {
        return this;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public String symbol() {
        return "LIKE";
    }
}
