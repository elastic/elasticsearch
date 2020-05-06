/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;

abstract class ByteTrackingSortingNumericDocValues extends AbstractSortingNumericDocValues {

    public long getValuesBytes() {
        return values.length * Long.BYTES;
    }
}
