/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.inference.InferenceObjectRamBytesUsedTest;

import java.util.List;

public class TextReasoningDetailRamBytesUsedTests extends InferenceObjectRamBytesUsedTest<ReasoningDetail.TextReasoningDetail> {

    private static final String FORMAT = "format";
    private static final String ID = "id";
    private static final Long INDEX = 1L;
    private static final String TEXT = "text";
    private static final String SIGNATURE = "signature";

    @Override
    public ReasoningDetail.TextReasoningDetail objectToEstimate() {
        return new ReasoningDetail.TextReasoningDetail(FORMAT, ID, INDEX, TEXT, SIGNATURE);
    }

    @Override
    public List<ReasoningDetail.TextReasoningDetail> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger format
            new ReasoningDetail.TextReasoningDetail(FORMAT.repeat(5), ID, INDEX, TEXT, SIGNATURE),
            // Larger id
            new ReasoningDetail.TextReasoningDetail(FORMAT, ID.repeat(5), INDEX, TEXT, SIGNATURE),
            // Larger text
            new ReasoningDetail.TextReasoningDetail(FORMAT, ID, INDEX, TEXT.repeat(5), SIGNATURE),
            // Larger signature
            new ReasoningDetail.TextReasoningDetail(FORMAT, ID, INDEX, TEXT, SIGNATURE.repeat(5))
        );
    }
}
