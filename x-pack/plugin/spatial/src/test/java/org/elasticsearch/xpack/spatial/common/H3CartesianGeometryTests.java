/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.common;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.h3.H3;
import org.elasticsearch.test.ESTestCase;

public class H3CartesianGeometryTests extends ESTestCase {
    public void testPolarH3Crossing() {
        long h3 = H3.geoToH3(-90, 0, 2);
        Component2D component2D = LatLonGeometry.create(H3CartesianUtil.getLatLonGeometry(h3));
        assertEquals(
            PointValues.Relation.CELL_CROSSES_QUERY,
            component2D.relate(-155.15317029319704, 163.99789148010314, -88.69673718698323, -88.26949733309448)
        );
    }
}
