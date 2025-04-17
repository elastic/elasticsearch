/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

/**
 * A hook for {@link DirectIOLucene99FlatVectorsReader} to specify the input should be opened using DirectIO.
 * Remove when IOContext allows more extensible payloads to be specified.
 */
public interface DirectIOIndexInputSupplier {
    IndexInput openInputDirect(String name, IOContext context) throws IOException;
}
