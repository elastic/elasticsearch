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

public class SummaryReasoningDetailRamBytesUsedTests extends InferenceObjectRamBytesUsedTest<ReasoningDetail.SummaryReasoningDetail> {

    private static final String FORMAT = "format";
    private static final String ID = "id";
    private static final Long INDEX = 1L;
    private static final String SUMMARY = "summary";

    @Override
    public ReasoningDetail.SummaryReasoningDetail objectToEstimate() {
        return new ReasoningDetail.SummaryReasoningDetail(FORMAT, ID, INDEX, SUMMARY);
    }

    @Override
    public List<ReasoningDetail.SummaryReasoningDetail> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger format
            new ReasoningDetail.SummaryReasoningDetail(FORMAT.repeat(5), ID, INDEX, SUMMARY),
            // Larger id
            new ReasoningDetail.SummaryReasoningDetail(FORMAT, ID.repeat(5), INDEX, SUMMARY),
            // Larger summary
            new ReasoningDetail.SummaryReasoningDetail(FORMAT, ID, INDEX, SUMMARY.repeat(5))
        );
    }
}
