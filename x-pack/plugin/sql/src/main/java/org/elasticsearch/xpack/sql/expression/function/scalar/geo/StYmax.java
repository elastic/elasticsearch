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
 * ST_YMax function that takes a geometry and returns max Y of all coordinates
 */
public class StYmax extends UnaryGeoFunction {

    public StYmax(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<StYmax> info() {
        return NodeInfo.create(this, StYmax::new, field());
    }

    @Override
    protected StYmax replaceChild(Expression newChild) {
        return new StYmax(location(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.Y_MAX;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

}
