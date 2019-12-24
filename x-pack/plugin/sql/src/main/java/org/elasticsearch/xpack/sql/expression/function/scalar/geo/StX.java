/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;


import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoProcessor.GeoOperation;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * ST_X function that takes a geometry and returns the X coordinate of its first point
 */
public class StX extends UnaryGeoFunction {

    public StX(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<StX> info() {
        return NodeInfo.create(this, StX::new, field());
    }

    @Override
    protected StX replaceChild(Expression newChild) {
        return new StX(source(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.X;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

}
