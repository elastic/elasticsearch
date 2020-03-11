/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.inference.search;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests;
import org.elasticsearch.xpack.ml.inference.search.InferenceSearchExtBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InferenceSearchExtBuilderTests extends AbstractSerializingTestCase<InferenceSearchExtBuilder> {
    @Override
    protected InferenceSearchExtBuilder doParseInstance(XContentParser parser) {
        return InferenceSearchExtBuilder.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<InferenceSearchExtBuilder> instanceReader() {
        return InferenceSearchExtBuilder::new;
    }

    @Override
    protected InferenceSearchExtBuilder createTestInstance() {
        InferenceConfig config;

        if (randomBoolean()) {
            config = ClassificationConfigTests.randomClassificationConfig();
        } else {
            config = RegressionConfigTests.randomRegressionConfig();
        }

        return new InferenceSearchExtBuilder(randomAlphaOfLength(8), Collections.singletonList(config),
                randomAlphaOfLength(6), randomMapOrNull());
    }

    private Map<String, String> randomMapOrNull() {
        if (randomBoolean()) {
            return null;
        }

        int numEntries = randomIntBetween(0, 3);
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < numEntries; i++) {
            result.put("field" + i, randomAlphaOfLength(5));
        }

        return result;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new MlInferenceNamedXContentProvider().getNamedWriteables());
    }
}
