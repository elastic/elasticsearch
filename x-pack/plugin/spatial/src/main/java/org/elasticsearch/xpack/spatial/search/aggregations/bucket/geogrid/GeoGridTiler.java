/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

/**
 * The tiler to use to convert a geo value into long-encoded bucket keys for aggregating.
 */
public abstract class GeoGridTiler {

    protected final int precision;

    public GeoGridTiler(int precision) {
        this.precision = precision;
    }

    /**
     * returns the precision of this tiler
     */
    public int precision() {
        return precision;
    }

    /**
     * encodes a single point to its long-encoded bucket key value.
     *
     * @param x        the x-coordinate
     * @param y        the y-coordinate
     */
    public abstract long encode(double x, double y);

    /**
     *
     * @param docValues        the array of long-encoded bucket keys to fill
     * @param geoValue         the input shape
     *
     * @return the number of cells the geoValue intersects
     */
    public abstract int setValues(GeoShapeCellValues docValues, GeoShapeValues.GeoShapeValue geoValue);

    /** Maximum number of cells that can be created by this tiler */
    protected abstract long getMaxCells();
}

