/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.index.NumericDocValues;

/**
 * An es819 doc values Specialization that allows retrieving a {@link BulkReader}.
 */
public abstract class BulkNumericDocValues extends NumericDocValues {

    /**
     * @return a bulk reader instance or <code>null</code> if field or implementation doesn't support bulk loading.
     */
    public abstract BulkReader getBulkReader();

}
