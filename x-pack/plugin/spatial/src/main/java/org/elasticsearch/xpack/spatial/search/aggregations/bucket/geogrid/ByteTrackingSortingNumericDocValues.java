/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;

import java.util.function.LongConsumer;

/**
 * Wrapper class for GeoGrid to expose the protected values array for testing
 */
abstract class ByteTrackingSortingNumericDocValues extends AbstractSortingNumericDocValues {

    ByteTrackingSortingNumericDocValues(LongConsumer circuitBreakerConsumer) {
        super(circuitBreakerConsumer);
    }

    long getValuesBytes() {
        return values.length * Long.BYTES;
    }
}
