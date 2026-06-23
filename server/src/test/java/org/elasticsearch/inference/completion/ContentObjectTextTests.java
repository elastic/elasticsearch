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

public class ContentObjectTextTests extends InferenceObjectRamBytesUsedTest<ContentObject.ContentObjectText> {

    private static final String TEXT = "text";

    @Override
    public ContentObject.ContentObjectText objectToEstimate() {
        return new ContentObject.ContentObjectText(TEXT);
    }

    @Override
    public List<ContentObject.ContentObjectText> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger text
            new ContentObject.ContentObjectText(TEXT.repeat(5))
        );
    }
}
