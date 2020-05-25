/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.xpack.spatial.index.fielddata.MultiGeoShapeValues;

/**
 * The tiler to use to convert a geo value into long-encoded bucket keys for aggregating.
 */
public interface GeoGridTiler {
    /**
     * encodes a single point to its long-encoded bucket key value.
     *
     * @param x        the x-coordinate
     * @param y        the y-coordinate
     * @param precision  the zoom level of tiles
     */
    long encode(double x, double y, int precision);

    /**
     *
     * @param docValues        the array of long-encoded bucket keys to fill
     * @param geoValue         the input shape
     * @param precision        the tile zoom-level
     *
     * @return the number of tiles the geoValue intersects
     */
    int setValues(GeoShapeCellValues docValues, MultiGeoShapeValues.GeoShapeValue geoValue, int precision);
}

