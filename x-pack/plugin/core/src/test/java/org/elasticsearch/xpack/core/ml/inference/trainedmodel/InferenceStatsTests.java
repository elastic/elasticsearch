/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;

import static org.elasticsearch.xpack.core.ml.utils.ToXContentParams.FOR_INTERNAL_STORAGE;

public class InferenceStatsTests extends AbstractXContentSerializingTestCase<InferenceStats> {

    public static InferenceStats createTestInstance(String modelId, @Nullable String nodeId) {
        return new InferenceStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            modelId,
            nodeId,
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
        return createTestInstance(randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected InferenceStats mutateInstance(InferenceStats instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<InferenceStats> instanceReader() {
        return InferenceStats::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(FOR_INTERNAL_STORAGE, "true"));
    }

}
