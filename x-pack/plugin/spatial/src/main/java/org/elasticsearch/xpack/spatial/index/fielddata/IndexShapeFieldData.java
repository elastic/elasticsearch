/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

/**
 * Specialization of {@link IndexFieldData} for geo shapes and shapes.
 */
public interface IndexShapeFieldData<T extends ToXContentFragment> extends IndexFieldData<LeafShapeFieldData<T>> {
    interface Geo extends IndexShapeFieldData<GeoPoint> {}

    interface Cartesian extends IndexShapeFieldData<CartesianPoint> {}
}
