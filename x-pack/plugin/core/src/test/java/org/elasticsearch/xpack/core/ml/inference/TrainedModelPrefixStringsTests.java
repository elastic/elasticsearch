/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class TrainedModelPrefixStringsTests extends AbstractXContentSerializingTestCase<TrainedModelPrefixStrings> {

    private boolean lenient = randomBoolean();

    public static TrainedModelPrefixStrings randomInstance() {
        boolean noNullMembers = randomBoolean();
        if (noNullMembers) {
            return new TrainedModelPrefixStrings(randomAlphaOfLength(5), randomAlphaOfLength(5));
        } else {
            boolean firstIsNull = randomBoolean();
            return new TrainedModelPrefixStrings(firstIsNull ? null : randomAlphaOfLength(5), firstIsNull ? randomAlphaOfLength(5) : null);
        }
    }

    @Override
    protected Writeable.Reader<TrainedModelPrefixStrings> instanceReader() {
        return TrainedModelPrefixStrings::new;
    }

    @Override
    protected TrainedModelPrefixStrings createTestInstance() {
        return randomInstance();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected TrainedModelPrefixStrings mutateInstance(TrainedModelPrefixStrings instance) throws IOException {
        return null;
    }

    @Override
    protected TrainedModelPrefixStrings doParseInstance(XContentParser parser) throws IOException {
        return TrainedModelPrefixStrings.fromXContent(parser, lenient);
    }
}
