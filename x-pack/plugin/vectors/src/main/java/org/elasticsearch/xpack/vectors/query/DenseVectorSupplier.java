/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.ScriptDocValues;

public interface DenseVectorSupplier extends ScriptDocValues.Supplier<BytesRef> {

    @Override
    default BytesRef getInternal(int index) {
        throw new UnsupportedOperationException();
    }

    DenseVector getInternal();
}
