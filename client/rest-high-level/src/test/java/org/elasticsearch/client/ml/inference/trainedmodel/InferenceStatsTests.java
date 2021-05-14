/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.inference.trainedmodel;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.time.Instant;

public class InferenceStatsTests extends AbstractXContentTestCase<InferenceStats> {

    public static InferenceStats randomInstance() {
        return new InferenceStats(randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            Instant.now()
            );
    }

    @Override
    protected InferenceStats doParseInstance(XContentParser parser) throws IOException {
        return InferenceStats.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected InferenceStats createTestInstance() {
        return randomInstance();
    }

}
