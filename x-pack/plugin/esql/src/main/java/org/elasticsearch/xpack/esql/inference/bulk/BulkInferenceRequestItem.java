/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.xpack.core.inference.action.InferenceAction;

/**
 * Encapsulates an inference request with its associated sequence number and shape metadata.
 * <p>
 * The sequence number is used for ordering responses in the bulk execution state.
 * <p>
 * The shape array tracks the number of retained values per position from the original data.
 * This enables reconstructing the original structure for aggregating results from multi-value fields.
 * Different operators retain different amounts: Completion/TextEmbedding retain at most 1 value per position,
 * while Rerank retains all values.
 */
public class BulkInferenceRequestItem {

    private static final long NO_SEQ_NO = -1L;
    private static final int[] EMPTY_SHAPE = new int[0];

    private final long seqNo;
    private final InferenceAction.Request request;

    /**
     * Number of retained values per position. shape[i] = 0 means null/skipped, N means N values retained.
     * Note: The returned array should not be modified by callers.
     */
    private final int[] shape;

    public BulkInferenceRequestItem(InferenceAction.Request request, int[] shape) {
        this(request, shape, NO_SEQ_NO);
    }

    public BulkInferenceRequestItem(InferenceAction.Request request) {
        this(request, request == null ? EMPTY_SHAPE : request.getInput().stream().mapToInt(r -> 1).toArray(), NO_SEQ_NO);
    }

    private BulkInferenceRequestItem(InferenceAction.Request request, int[] shape, long seqNo) {
        this.seqNo = seqNo;
        this.request = request;
        this.shape = shape;
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
     * Returns the shape array: number of retained values per position from the original data.
     * <p>
     * Note: The returned array should not be modified.
     *
     * @return the shape array, never null
     */
    public int[] shape() {
        return shape;
    }

    /**
     * Returns a copy of the {@link BulkInferenceRequestItem} with the given sequence number.
     */
    public BulkInferenceRequestItem withSeqNo(long seqNo) {
        return new BulkInferenceRequestItem(request, shape, seqNo);
    }

}
