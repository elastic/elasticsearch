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

public class ContentObjectsTests extends InferenceObjectRamBytesUsedTest<ContentObjects> {

    private static final ContentObject CONTENT_OBJECT = new ContentObject.ContentObjectText("text");

    @Override
    public ContentObjects objectToEstimate() {
        return new ContentObjects(List.of(CONTENT_OBJECT));
    }

    @Override
    public List<ContentObjects> objectsToEstimateWithLargerInput() {
        return List.of(
            // More content objects
            new ContentObjects(List.of(CONTENT_OBJECT, CONTENT_OBJECT))
        );
    }
}
