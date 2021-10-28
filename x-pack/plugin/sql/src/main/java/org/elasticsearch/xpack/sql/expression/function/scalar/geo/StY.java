/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoProcessor.GeoOperation;

/**
 * ST_Y function that takes a geometry and returns the Y coordinate of its first point
 */
public class StY extends UnaryGeoFunction {

    public StY(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<StY> info() {
        return NodeInfo.create(this, StY::new, field());
    }

    @Override
    protected StY replaceChild(Expression newChild) {
        return new StY(source(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.Y;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

}
