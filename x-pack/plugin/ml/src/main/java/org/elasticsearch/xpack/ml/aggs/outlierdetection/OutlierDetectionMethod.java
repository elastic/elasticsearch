/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import java.util.Locale;

/**
 * Outlier detection scoring methods.
 * <ul>
 *   <li><b>KTH_NN</b> — distance to the k-th nearest neighbor</li>
 *   <li><b>TNN</b> — average distance to the k nearest neighbors</li>
 *   <li><b>LDOF</b> — Local Distance-based Outlier Factor (TNN / KNN inner distance)</li>
 *   <li><b>LOF</b> — Local Outlier Factor (density-based)</li>
 * </ul>
 */
public enum OutlierDetectionMethod {

    KTH_NN,
    TNN,
    LDOF,
    LOF;

    public static final OutlierDetectionMethod DEFAULT = KTH_NN;

    public static OutlierDetectionMethod fromString(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
