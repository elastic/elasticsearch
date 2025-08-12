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
import org.elasticsearch.index.mapper.BlockLoader;

/**
 * An es819 doc values specialization that allows retrieving a {@link BlockLoader.ColumnAtATimeReader}.
 */
public abstract class BulkNumericDocValues extends NumericDocValues {

    /**
     * @return a column at a time reader or <code>null</code> if field or implementation doesn't support a column at a time reader.
     */
    public abstract BlockLoader.ColumnAtATimeReader getColumnAtATimeReader();

}
