/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.common.util.FeatureFlag;

/**
 * Feature flags and settings for bulk vector processing optimizations.
 */
public final class BulkVectorProcessingSettings {

    public static final boolean BULK_VECTOR_SCORING = new FeatureFlag("bulk_vector_scoring").isEnabled();

    public static final int MIN_BULK_PROCESSING_THRESHOLD = 3;

    private BulkVectorProcessingSettings() {
        // Utility class
    }

    public static boolean shouldUseBulkProcessing(int documentCount) {
        return BULK_VECTOR_SCORING && documentCount >= MIN_BULK_PROCESSING_THRESHOLD;
    }
}
