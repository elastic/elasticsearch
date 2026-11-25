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

public class ContentObjectFileFieldTests extends InferenceObjectRamBytesUsedTest<ContentObject.ContentObjectFile.ContentObjectFileFields> {

    private static final String FILE_DATA = "file data";
    private static final String FILE_ID = "file id";
    private static final String FILE_NAME = "file name";

    @Override
    public ContentObject.ContentObjectFile.ContentObjectFileFields objectToEstimate() {
        return new ContentObject.ContentObjectFile.ContentObjectFileFields(FILE_DATA, FILE_ID, FILE_NAME);
    }

    @Override
    public List<ContentObject.ContentObjectFile.ContentObjectFileFields> objectsToEstimateWithLargerInput() {
        return List.of(
            // Larger file data
            new ContentObject.ContentObjectFile.ContentObjectFileFields(FILE_DATA.repeat(5), FILE_ID, FILE_NAME),
            // Larger file id
            new ContentObject.ContentObjectFile.ContentObjectFileFields(FILE_DATA, FILE_ID.repeat(5), FILE_NAME),
            // Larger file name
            new ContentObject.ContentObjectFile.ContentObjectFileFields(FILE_DATA, FILE_ID, FILE_NAME.repeat(5))
        );
    }
}
