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

public class EncryptedReasoningDetailRamBytesUsedTests extends InferenceObjectRamBytesUsedTest<ReasoningDetail.EncryptedReasoningDetail> {

    private static final String FORMAT = "format";
    private static final String ID = "id";
    private static final Long INDEX = 1L;
    private static final String DATA = "data";

    @Override
    public ReasoningDetail.EncryptedReasoningDetail objectToEstimate() {
        return new ReasoningDetail.EncryptedReasoningDetail(FORMAT, ID, INDEX, DATA);
    }

    @Override
    public List<ReasoningDetail.EncryptedReasoningDetail> objectsToEstimateWithLargerInput() {
        return List.of(
            // Longer format
            new ReasoningDetail.EncryptedReasoningDetail(FORMAT.repeat(5), ID, INDEX, DATA),
            // Longer id
            new ReasoningDetail.EncryptedReasoningDetail(FORMAT, ID.repeat(5), INDEX, DATA),
            // Longer data
            new ReasoningDetail.EncryptedReasoningDetail(FORMAT, ID, INDEX, DATA.repeat(5))
        );
    }
}
