/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import java.util.Locale;

public enum SimilarityMeasure {
    COSINE,
    DOT_PRODUCT,
    L2_NORM;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static SimilarityMeasure fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
