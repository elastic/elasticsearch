/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.Iterator;

public interface BulkInferenceRequestIterator extends Iterator<InferenceAction.Request>, Releasable {

    /**
     * Returns an estimate of the number of requests that will be produced.
     *
     * <p>This is typically used to pre-allocate buffers or output to th appropriate size.</p>
     */
    int estimatedSize();

}
