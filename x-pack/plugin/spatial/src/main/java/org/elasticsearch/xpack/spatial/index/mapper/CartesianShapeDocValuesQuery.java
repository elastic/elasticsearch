/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYGeometry;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;

/** Lucene geometry query for {@link BinaryShapeDocValuesField}. */
public class CartesianShapeDocValuesQuery extends ShapeDocValuesQuery<XYGeometry> {

    public CartesianShapeDocValuesQuery(String field, ShapeField.QueryRelation relation, XYGeometry... geometries) {
        super(field, CoordinateEncoder.CARTESIAN, relation, geometries);
    }

    @Override
    protected Component2D create(XYGeometry[] geometries) {
        return XYGeometry.create(geometries);
    }

    @Override
    protected Component2D create(XYGeometry geometry) {
        return XYGeometry.create(geometry);
    }
}
