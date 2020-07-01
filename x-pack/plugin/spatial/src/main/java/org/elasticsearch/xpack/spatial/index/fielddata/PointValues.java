/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;

/**
 * Per-document XY point values.
 */
public abstract class PointValues {

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Get the {@link CartesianPoint} associated with the current document.
     * The returned {@link CartesianPoint} might be reused across calls.
     */
    public abstract CartesianPoint pointValue();

}
