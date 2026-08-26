/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.index.FloatVectorValues;

import java.io.IOException;

public abstract class BFloat16VectorValues extends FloatVectorValues {
    /**
     * Returns the bfloat16 vector bytes for the given ordinal.
     */
    public abstract byte[] bfloat16VectorBytes(int targetOrd) throws IOException;
}
