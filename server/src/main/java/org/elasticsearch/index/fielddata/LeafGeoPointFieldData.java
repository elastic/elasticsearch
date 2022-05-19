/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;

/**
 * {@link LeafFieldData} specialization for geo points.
 */
public interface LeafGeoPointFieldData extends LeafFieldData {

    /**
     * Return geo-point values.
     */
    MultiGeoPointValues getGeoPointValues();

    /**
     * Return internal implementation.
     */
    SortedNumericDocValues getSortedNumericDocValues();

}
