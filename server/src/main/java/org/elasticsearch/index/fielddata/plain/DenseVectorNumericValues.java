/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.KnnVectorValues;
import org.elasticsearch.index.fielddata.FormattedDocValues;

public abstract class DenseVectorNumericValues implements FormattedDocValues {

    protected final int dims;
    protected int valuesCursor;

    protected KnnVectorValues.DocIndexIterator iterator; // use when indexed
    protected BinaryDocValues binary; // use when not indexed

    protected DenseVectorNumericValues(int dims) {
        this.dims = dims;
        valuesCursor = 0;
    }

    @Override
    public final int docValueCount() {
        return dims;
    }
}
