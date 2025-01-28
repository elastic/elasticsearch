/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.XYGeometry;

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
