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
 * ST_GEOMETRY_TYPE function that takes a geometry and returns its type
 */
public class StGeometryType extends UnaryGeoFunction {

    public StGeometryType(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<StGeometryType> info() {
        return NodeInfo.create(this, StGeometryType::new, field());
    }

    @Override
    protected StGeometryType replaceChild(Expression newChild) {
        return new StGeometryType(source(), newChild);
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
