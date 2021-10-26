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
 * ST_Z function that takes a geometry and returns the Z coordinate of its first point
 */
public class StZ extends UnaryGeoFunction {

    public StZ(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<StZ> info() {
        return NodeInfo.create(this, StZ::new, field());
    }

    @Override
    protected StZ replaceChild(Expression newChild) {
        return new StZ(source(), newChild);
    }

    @Override
    protected GeoOperation operation() {
        return GeoOperation.Z;
    }

    @Override
    public DataType dataType() {
        return DataTypes.DOUBLE;
    }

}
