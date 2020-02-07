/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.inference.search;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.rescore.QueryRescoreMode;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfigTests;

import java.util.HashMap;
import java.util.Map;

public class InferenceRescorerBuilderTests extends AbstractSerializingTestCase<InferenceRescorerBuilder> {

    @Override
    protected InferenceRescorerBuilder doParseInstance(XContentParser parser) {
        return InferenceRescorerBuilder.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<InferenceRescorerBuilder> instanceReader() {
        return InferenceRescorerBuilder::new;
    }

    @Override
    protected InferenceRescorerBuilder createTestInstance() {
        InferenceConfig config = null;

        if (randomBoolean()) {
            if (randomBoolean()) {
                config = ClassificationConfigTests.randomClassificationConfig();
            } else {
                config = RegressionConfigTests.randomRegressionConfig();
            }
        }

        InferenceRescorerBuilder builder = new InferenceRescorerBuilder(randomAlphaOfLength(8), config, randomMap());
        if (randomBoolean()) {
            builder.setQueryWeight((float) randomDoubleBetween(0.0, 1.0, true));
            builder.setModelWeight((float) randomDoubleBetween(0.0, 2.0, true));
            builder.setScoreMode(randomFrom(QueryRescoreMode.values()));
        }

        return builder;
    }

    private Map<String, String> randomMap() {
        int numEntries = randomIntBetween(0, 6);
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
