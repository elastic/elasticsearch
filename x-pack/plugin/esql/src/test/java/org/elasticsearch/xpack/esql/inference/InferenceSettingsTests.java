/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class InferenceSettingsTests extends ESTestCase {

    public void testDenseVectorBatchSizeDefaultsToConstant() {
        assertThat(
            new InferenceSettings(Settings.EMPTY).denseVectorBatchSize(),
            equalTo(InferenceSettings.DENSE_VECTOR_DEFAULT_BATCH_SIZE)
        );
    }

    public void testDenseVectorBatchSizeReadsConfiguredValue() {
        int batchSize = between(1, InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE);
        Settings settings = Settings.builder().put(InferenceSettings.DENSE_VECTOR_BATCH_SIZE_SETTING.getKey(), batchSize).build();
        assertThat(new InferenceSettings(settings).denseVectorBatchSize(), equalTo(batchSize));
    }

    /**
     * Both ends of the accepted range are valid: the minimum (1) and the cap itself
     * ({@link InferenceSettings#DENSE_VECTOR_MAX_BATCH_SIZE}). Only values strictly outside the range are rejected.
     */
    public void testDenseVectorBatchSizeAcceptsBoundaryValues() {
        assertThat(batchSizeFor(1), equalTo(1));
        assertThat(batchSizeFor(InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE), equalTo(InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE));
    }

    public void testDenseVectorBatchSizeRejectsZero() {
        assertBatchSizeRejected(0, "must be >= 1");
    }

    public void testDenseVectorBatchSizeRejectsNegativeOne() {
        assertBatchSizeRejected(-1, "must be >= 1");
    }

    public void testDenseVectorBatchSizeRejectsValueAboveMax() {
        int aboveMax = InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE + between(1, 100);
        assertBatchSizeRejected(aboveMax, "must be <= " + InferenceSettings.DENSE_VECTOR_MAX_BATCH_SIZE);
    }

    private static int batchSizeFor(int batchSize) {
        Settings settings = Settings.builder().put(InferenceSettings.DENSE_VECTOR_BATCH_SIZE_SETTING.getKey(), batchSize).build();
        return new InferenceSettings(settings).denseVectorBatchSize();
    }

    private static void assertBatchSizeRejected(int batchSize, String boundMessage) {
        Settings settings = Settings.builder().put(InferenceSettings.DENSE_VECTOR_BATCH_SIZE_SETTING.getKey(), batchSize).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new InferenceSettings(settings));
        assertThat(e.getMessage(), containsString(InferenceSettings.DENSE_VECTOR_BATCH_SIZE_SETTING.getKey()));
        assertThat(e.getMessage(), containsString(boundMessage));
    }
}
