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
 * ST_XMax function that takes a geometry and returns max X of all coordinates
 */
public class StXmax extends UnaryGeoFunction {

    public StXmax(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<StXmax> info() {
        return NodeInfo.create(this, StXmax::new, field());
    }

    @Override
    protected StXmax replaceChild(Expression newChild) {
        return new StXmax(location(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.X_MAX;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

}
