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

public class ContentObjectImageUrlTests extends InferenceObjectRamBytesUsedTest<ContentObject.ContentObjectImage.ContentObjectImageUrl> {

    private static final String URL = "some image url";

    @Override
    public ContentObject.ContentObjectImage.ContentObjectImageUrl objectToEstimate() {
        return new ContentObject.ContentObjectImage.ContentObjectImageUrl(URL, ContentObject.ContentObjectImage.ContentObjectImageUrl.ImageUrlDetail.AUTO);
    }

    @Override
    public List<ContentObject.ContentObjectImage.ContentObjectImageUrl> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger URL
            new ContentObject.ContentObjectImage.ContentObjectImageUrl(URL.repeat(5), ContentObject.ContentObjectImage.ContentObjectImageUrl.ImageUrlDetail.AUTO)
        );
    }
}
