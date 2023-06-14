/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.io.IOException;
import java.util.function.Supplier;

public class InferenceRescorerBuilderSerializationTests extends AbstractBWCSerializationTestCase<InferenceRescorerBuilder> {

    @Override
    protected InferenceRescorerBuilder doParseInstance(XContentParser parser) throws IOException {
        XContentParser.Token next = parser.nextToken();
        assert next == XContentParser.Token.START_OBJECT;
        next = parser.nextToken();
        assert next == XContentParser.Token.FIELD_NAME;
        return InferenceRescorerBuilder.fromXContent(parser, null);
    }

    @Override
    protected Writeable.Reader<InferenceRescorerBuilder> instanceReader() {
        return in -> new InferenceRescorerBuilder(in, null);
    }

    @Override
    protected InferenceRescorerBuilder createTestInstance() {
        return new InferenceRescorerBuilder(randomAlphaOfLength(10), (Supplier<ModelLoadingService>) null);
    }

    @Override
    protected InferenceRescorerBuilder mutateInstance(InferenceRescorerBuilder instance) throws IOException {
        return new InferenceRescorerBuilder(randomAlphaOfLength(10), (Supplier<ModelLoadingService>) null);
    }

    @Override
    protected InferenceRescorerBuilder mutateInstanceForVersion(InferenceRescorerBuilder instance, TransportVersion version) {
        return instance;
    }
}
