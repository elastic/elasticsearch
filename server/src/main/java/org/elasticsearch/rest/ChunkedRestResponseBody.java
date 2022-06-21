/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.rest;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.recycler.Recycler;

import java.io.IOException;
import java.util.function.Consumer;

public interface ChunkedRestResponseBody {

    boolean encode(Consumer<ReleasableBytesReference> target, int sizeHint, Recycler<BytesRef> recycler) throws IOException;
}
