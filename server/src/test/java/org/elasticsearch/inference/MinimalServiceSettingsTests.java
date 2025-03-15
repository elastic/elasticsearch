/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class MinimalServiceSettingsTests extends AbstractXContentTestCase<MinimalServiceSettings> {
    public static MinimalServiceSettings randomInstance() {
        TaskType taskType = randomFrom(TaskType.values());
        Integer dimensions = null;
        SimilarityMeasure similarity = null;
        DenseVectorFieldMapper.ElementType elementType = null;

        if (taskType == TaskType.TEXT_EMBEDDING) {
            dimensions = randomIntBetween(2, 1024);
            similarity = randomFrom(SimilarityMeasure.values());
            elementType = randomFrom(DenseVectorFieldMapper.ElementType.values());
        }
        return new MinimalServiceSettings(randomBoolean() ? null : randomAlphaOfLength(10), taskType, dimensions, similarity, elementType);
    }

    @Override
    protected MinimalServiceSettings createTestInstance() {
        return randomInstance();
    }

    @Override
    protected MinimalServiceSettings doParseInstance(XContentParser parser) throws IOException {
        return MinimalServiceSettings.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
