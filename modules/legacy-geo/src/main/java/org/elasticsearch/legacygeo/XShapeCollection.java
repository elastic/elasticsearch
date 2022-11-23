/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

import java.util.List;

/**
 * Extends spatial4j ShapeCollection for points_only shape indexing support
 */
public class XShapeCollection<S extends Shape> extends ShapeCollection<S> {

    private boolean pointsOnly = false;

    public XShapeCollection(List<S> shapes, SpatialContext ctx) {
        super(shapes, ctx);
    }

    public boolean pointsOnly() {
        return this.pointsOnly;
    }

    public void setPointsOnly(boolean pointsOnly) {
        this.pointsOnly = pointsOnly;
    }
}
