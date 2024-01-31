/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors.codec;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ScalarQuantizer;

import java.io.Closeable;
import java.io.IOException;

interface ESQuantizedVectorsReader extends Closeable, Accountable {

    ESQuantizedByteVectorValues getQuantizedVectorValues(String fieldName) throws IOException;

    ScalarQuantizer getQuantizationState(String fieldName);
}
