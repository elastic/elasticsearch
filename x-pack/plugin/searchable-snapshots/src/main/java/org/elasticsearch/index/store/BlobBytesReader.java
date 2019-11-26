/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import java.io.IOException;

public interface BlobBytesReader {

    void readBlobBytes(String name, long from, int length, byte[] buffer, int offset) throws IOException;
}
