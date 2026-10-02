/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.elasticsearch.core.Nullable;

public final class IvfAutoCalibrationOsqParams {
    /**
     * Default target recall for calibration sweeps.
     */
    static final double DEFAULT_TARGET_RECALL = 0.9;

    /**
     * Default number of nearest neighbors {@code k} used in recall estimation during calibration.
     */
    static final int DEFAULT_K = 10;

    /**
     * Doc-bit ceiling that leaves every candidate encoding available, i.e. no effective cap.
     */
    static final int UNCAPPED_MAX_DOC_BITS = 7;

    private final double targetRecall;
    private final int k;
    private final int maxDocBits;

    IvfAutoCalibrationOsqParams(@Nullable Double targetRecall, @Nullable Integer k, @Nullable Integer maxDocBits) {
        this.targetRecall = targetRecall == null ? DEFAULT_TARGET_RECALL : targetRecall;
        this.k = k == null ? DEFAULT_K : k;
        this.maxDocBits = maxDocBits == null ? UNCAPPED_MAX_DOC_BITS : maxDocBits;
    }

    public double targetRecall() {
        return targetRecall;
    }

    public int k() {
        return k;
    }

    public int maxDocBits() {
        return maxDocBits;
    }
}
