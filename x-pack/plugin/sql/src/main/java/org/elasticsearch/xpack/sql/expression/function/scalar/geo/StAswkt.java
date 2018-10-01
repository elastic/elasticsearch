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
 * ST_AsWKT function that takes a geometry and returns its Well Known Text representation
 */
public class StAswkt extends UnaryGeoFunction {

    public StAswkt(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<StAswkt> info() {
        return NodeInfo.create(this, StAswkt::new, field());
    }

    @Override
    protected StAswkt replaceChild(Expression newChild) {
        return new StAswkt(location(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        if (field().dataType() == DataType.GEO_POINT) {
            return GeoOperation.ASWKT_POINT;
        } else {
            return GeoOperation.ASWKT_SHAPE;
        }
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

}
