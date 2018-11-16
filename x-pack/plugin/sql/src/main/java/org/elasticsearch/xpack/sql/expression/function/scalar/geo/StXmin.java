/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.function.scalar.geo;


import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.function.scalar.geo.GeoProcessor.GeoOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * ST_XMin function that takes a geometry and returns min X of all coordinates
 */
public class StXmin extends UnaryGeoFunction {

    public StXmin(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<StXmin> info() {
        return NodeInfo.create(this, StXmin::new, field());
    }

    @Override
    protected StXmin replaceChild(Expression newChild) {
        return new StXmin(location(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.X_MIN;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

}
