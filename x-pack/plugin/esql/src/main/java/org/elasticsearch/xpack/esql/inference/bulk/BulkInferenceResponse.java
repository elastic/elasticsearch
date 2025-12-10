/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference.bulk;

import org.elasticsearch.xpack.core.inference.action.InferenceAction;

public class BulkInferenceResponse {

    private final InferenceAction.Response response;
    private final int[] shape;
    private final long seqNo;

    public BulkInferenceResponse(BulkInferenceRequestItem request, InferenceAction.Response response) {
        this.seqNo = request.seqNo();
        this.shape = request.shape();
        this.response = response;
    }

    /**
     * Returns the inference response seq number.
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the inference response.
     */
    public InferenceAction.Response response() {
        return response;
    }

    public int[] shape() {
        return shape;
    }
}
