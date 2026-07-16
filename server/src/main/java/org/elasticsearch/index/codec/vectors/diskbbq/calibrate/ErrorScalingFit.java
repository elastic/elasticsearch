/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

/**
 * Result of the error-model scaling step: the fitted slope at fixed query-bits/doc-bits encoding,
 * plus clustering warm-start state reused by {@code ErrorModel#estimateMagnitudeModel}.
 */
public final class ErrorScalingFit {

    private final QuantizationErrorStdModel scalingModel;
    final float[][] lastDocCentroids;
    final float[][] lastQueryCentroids;

    ErrorScalingFit(QuantizationErrorStdModel scalingModel, float[][] lastDocCentroids, float[][] lastQueryCentroids) {
        this.scalingModel = scalingModel;
        this.lastDocCentroids = lastDocCentroids;
        this.lastQueryCentroids = lastQueryCentroids;
    }

    /**
     * Scaling regression fit; slope ({@code beta1}) is reused when fitting magnitude for other encodings.
     */
    public QuantizationErrorStdModel scalingModel() {
        return scalingModel;
    }

    /**
     * Scaling model without clustering warm-start state.
     */
    public static ErrorScalingFit fromScalingModel(QuantizationErrorStdModel scalingModel) {
        return new ErrorScalingFit(scalingModel, null, null);
    }
}
