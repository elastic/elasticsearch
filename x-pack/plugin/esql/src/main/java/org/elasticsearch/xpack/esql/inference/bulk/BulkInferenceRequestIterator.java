/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.core.Releasable;

import java.util.Iterator;

public interface BulkInferenceRequestIterator extends Iterator<BulkInferenceRequestItem<?>>, Releasable {

    /**
     * Returns an estimate of the number of requests that will be produced.
     *
     * <p>This is typically used to pre-allocate buffers or output to the appropriate size.</p>
     */
    int estimatedSize();
}
