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
 * ST_GeometryType function that takes a geometry and returns its type
 */
public class StGeometrytype extends UnaryGeoFunction {

    public StGeometrytype(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<StGeometrytype> info() {
        return NodeInfo.create(this, StGeometrytype::new, field());
    }

    @Override
    protected StGeometrytype replaceChild(Expression newChild) {
        return new StGeometrytype(location(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.GEOMETRY_TYPE;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

}
