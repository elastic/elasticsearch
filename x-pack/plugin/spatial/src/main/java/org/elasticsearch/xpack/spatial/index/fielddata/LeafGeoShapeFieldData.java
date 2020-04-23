/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.elasticsearch.index.fielddata.LeafFieldData;

/**
 * {@link LeafFieldData} specialization for geo points and shapes.
 */
public interface LeafGeoShapeFieldData extends LeafFieldData {
    /**
     * Return geo shape values.
     */
    MultiGeoShapeValues getGeoShapeValues();

}
