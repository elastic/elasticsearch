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

public class ContentObjectFileTests extends InferenceObjectRamBytesUsedTest<ContentObject.ContentObjectFile> {

    @Override
    public ContentObject.ContentObjectFile objectToEstimate() {
        return new ContentObject.ContentObjectFile(new ContentObject.ContentObjectFile.ContentObjectFileFields("data", "id", "name"));
    }

    @Override
    public List<ContentObject.ContentObjectFile> objectsToEstimateWithLargerInput() {
        return List.of();
    }

    /**
      * Increasing inputs of {@link org.elasticsearch.inference.completion.ContentObject.ContentObjectFile.ContentObjectFileFields}
     *  are tested in {@link ContentObjectFileFieldTests}
      */
    @Override
    public boolean hasGrowingInputs() {
        return false;
    }
}
