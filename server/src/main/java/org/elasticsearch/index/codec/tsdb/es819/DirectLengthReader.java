/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import java.io.IOException;

public interface DirectLengthReader {
    /**
     * If a BinaryDocValues is used as a DirectLengthReader, the BytesRef itself cannot be accessed.
     * Calls to both getLength and binaryValue will invalid the BinaryDocValues.
     *
     * @return returns the length of the current BytesRefs
     * @throws IOException
     */
    int getLength() throws IOException;
}
