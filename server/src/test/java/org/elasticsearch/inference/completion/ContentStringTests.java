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

public class ContentStringTests extends InferenceObjectRamBytesUsedTest<ContentString> {

    private static final String CONTENT = "content";

    @Override
    public ContentString objectToEstimate() {
        return new ContentString(CONTENT);
    }

    @Override
    public List<ContentString> objectsToEstimateWithLargerInput() {
        return List.of(new ContentString(CONTENT.repeat(10)));
    }
}
