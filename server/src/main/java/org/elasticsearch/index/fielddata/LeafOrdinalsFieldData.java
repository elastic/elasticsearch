/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.SortedSetDocValues;

/**
 * Specialization of {@link LeafFieldData} for data that is indexed with
 * ordinals.
 */
public interface LeafOrdinalsFieldData extends LeafFieldData {

    /**
     * Return the ordinals values for the current atomic reader.
     */
    SortedSetDocValues getOrdinalsValues();

}
