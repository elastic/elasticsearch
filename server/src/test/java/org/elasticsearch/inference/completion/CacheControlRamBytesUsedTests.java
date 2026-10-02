/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceObjectRamBytesUsedTest;

import java.util.List;

public class CacheControlRamBytesUsedTests extends InferenceObjectRamBytesUsedTest<CacheControl> {

    @Override
    public CacheControl objectToEstimate() {
        return new CacheControl("ephemeral", TimeValue.ONE_HOUR);
    }

    @Override
    public List<CacheControl> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger type
            new CacheControl("some larger type", TimeValue.ONE_HOUR)
        );
    }
}
