/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.xpack.core.inference.action.InferenceAction;

/**
 * Encapsulates an inference request with its associated sequence number.
 * <p>
 * The sequence number is used for ordering responses and tracking completion
 * in the bulk execution state.
 * </p>
 */
public class BulkRequestItem {

    private static final long NO_SEQ_NO = -1L;

    private final long seqNo;
    private final InferenceAction.Request request;

    public BulkRequestItem(InferenceAction.Request request) {
        this(request, NO_SEQ_NO);
    }

    private BulkRequestItem(InferenceAction.Request request, long seqNo) {
        this.seqNo = seqNo;
        this.request = request;
    }

    /**
     * Returns the inference request seq number.
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the inference request.
     */
    public InferenceAction.Request request() {
        return request;
    }

    /**
     * Returns a copy of the {@link BulkRequestItem} with the given sequence number.
     */
    public BulkRequestItem withSeqNo(long seqNo) {
        return new BulkRequestItem(request, seqNo);
    }

}
