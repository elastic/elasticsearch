/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.mapper.fielddata;

import org.elasticsearch.index.fielddata.LeafFieldData;

import java.io.IOException;

/**
 * {@link LeafFieldData} specialization for Hyperloglog data.
 */
public interface LeafHllFieldData extends LeafFieldData {

    /**
     * Return Hyperloglog values.
     */
    HllValues getHllValues() throws IOException;

}
