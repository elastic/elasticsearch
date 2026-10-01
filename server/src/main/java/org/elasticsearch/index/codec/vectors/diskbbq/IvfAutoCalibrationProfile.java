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

import java.util.Locale;

public enum IvfAutoCalibrationProfile {
    DISABLED(null),
    ISO_SIZING(new IvfAutoCalibrationOsqParams(null, null, 1)),
    QUALITY(new IvfAutoCalibrationOsqParams(null, null, null));

    @Nullable
    private final IvfAutoCalibrationOsqParams osqParams;

    IvfAutoCalibrationProfile(@Nullable IvfAutoCalibrationOsqParams osqParams) {
        this.osqParams = osqParams;
    }

    public IvfAutoCalibrationOsqParams osqParams() {
        if (osqParams == null) {
            throw new IllegalStateException("No osq params for autocalibration profile [" + this + "]");
        }
        return osqParams;
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
