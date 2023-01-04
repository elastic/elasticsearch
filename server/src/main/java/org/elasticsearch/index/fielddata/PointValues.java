/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.common.geo.SpatialPoint;

import java.io.IOException;

/**
 * Per-document geo-point or point values.
 */
public abstract class PointValues<T extends SpatialPoint> {

    protected final NumericDocValues values;

    protected PointValues(NumericDocValues values) {
        this.values = values;
    }

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public boolean advanceExact(int doc) throws IOException {
        return values.advanceExact(doc);
    }

    public abstract T pointValue() throws IOException;
}
