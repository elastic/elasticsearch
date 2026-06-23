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

public class ContentObjectImageTests extends InferenceObjectRamBytesUsedTest<ContentObject.ContentObjectImage> {
    @Override
    public ContentObject.ContentObjectImage objectToEstimate() {
        return new ContentObject.ContentObjectImage(
            new ContentObject.ContentObjectImage.ContentObjectImageUrl(
                "url",
                ContentObject.ContentObjectImage.ContentObjectImageUrl.ImageUrlDetail.AUTO
            )
        );
    }

    @Override
    public List<ContentObject.ContentObjectImage> objectsToEstimateWithLargerInput() {
        // Not executed
        return List.of();
    }

    /**
     * Growing inputs of {@link org.elasticsearch.inference.completion.ContentObject.ContentObjectImage.ContentObjectImageUrl}
     * are tested in {@link ContentObjectImageUrlTests}
     */
    @Override
    public boolean hasGrowingInputs() {
        return false;
    }
}
