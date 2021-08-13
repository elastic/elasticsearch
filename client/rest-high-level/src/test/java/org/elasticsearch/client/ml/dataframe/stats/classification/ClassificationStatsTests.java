/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.stats.classification;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;

public class ClassificationStatsTests extends AbstractXContentTestCase<ClassificationStats> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected ClassificationStats doParseInstance(XContentParser parser) throws IOException {
        return ClassificationStats.PARSER.apply(parser, null);
    }

    @Override
    protected ClassificationStats createTestInstance() {
        return createRandom();
    }

    public static ClassificationStats createRandom() {
        return new ClassificationStats(
            Instant.now(),
            randomBoolean() ? null : randomIntBetween(1, Integer.MAX_VALUE),
            HyperparametersTests.createRandom(),
            TimingStatsTests.createRandom(),
            ValidationLossTests.createRandom()
        );
    }
}
