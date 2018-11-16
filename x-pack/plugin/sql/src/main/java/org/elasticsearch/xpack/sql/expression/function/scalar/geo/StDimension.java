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
 * ST_Dimension function that takes a geometry and returns its dimension
 */
public class StDimension extends UnaryGeoFunction {

    public StDimension(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<StDimension> info() {
        return NodeInfo.create(this, StDimension::new, field());
    }

    @Override
    protected StDimension replaceChild(Expression newChild) {
        return new StDimension(location(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.DIMENSION;
    }

    @Override
    public DataType dataType() {
        return DataType.SHORT;
    }

}
