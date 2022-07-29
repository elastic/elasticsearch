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
 * Specialization of {@link LeafFieldData} for numeric data.
 */
public interface LeafNumericFieldData extends LeafFieldData {

    /**
     * Get an integer view of the values of this segment. If the implementation
     * stores floating-point numbers then these values will return the same
     * values but casted to longs.
     */
    SortedNumericDocValues getLongValues();

    /**
     * Return a floating-point view of the values in this segment. If the
     * implementation stored integers then the returned doubles would be the
     * same ones as you would get from casting to a double.
     */
    SortedNumericDoubleValues getDoubleValues();

}
