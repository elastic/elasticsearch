/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

/**
 * Lets {@link IngestService} charge the memory that ingest pipelines add to the documents of a bulk request, without depending on how
 * (or whether) the caller accounts for it. Calls may come concurrently from any thread that completes a document's pipeline. Charged
 * bytes are held by the caller until the bulk completes, so there is no way to give them back here.
 */
public interface IngestMemoryAccountant {

    /** An accountant that accepts everything and keeps no state. */
    IngestMemoryAccountant NOOP = bytes -> {};

    /**
     * Charges bytes that stay reachable until the bulk request completes. This is the growth of a document beyond the size it was
     * admitted with, not the full size of the grown document.
     *
     * @throws EsRejectedExecutionException if the bytes do not fit, in which case nothing was charged and the document that caused them
     *                                      is failed
     */
    void add(long bytes) throws EsRejectedExecutionException;
}
