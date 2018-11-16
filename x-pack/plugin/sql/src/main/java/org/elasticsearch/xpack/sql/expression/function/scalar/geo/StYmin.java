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
 * ST_YMin function that takes a geometry and returns min Y of all coordinates
 */
public class StYmin extends UnaryGeoFunction {

    public StYmin(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<StYmin> info() {
        return NodeInfo.create(this, StYmin::new, field());
    }

    @Override
    protected StYmin replaceChild(Expression newChild) {
        return new StYmin(location(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.Y_MIN;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

}
