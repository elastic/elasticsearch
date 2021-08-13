/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;

public class DenseVectorScriptValue {
    private final Version indexVersion;
    private final int dims;
    private final float[] vector;
    private BytesRef value;

    public DenseVectorScriptValue(BinaryDocValues in, Version indexVersion, int dims) {
        this.indexVersion = indexVersion;
        this.dims = dims;
        this.vector = new float[dims];
    }


}
