/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.nlp;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class NlpTaskConfigTests extends AbstractXContentTestCase<NlpTaskConfig> {

    private final boolean lenient = randomBoolean();

    @Override
    protected NlpTaskConfig createTestInstance() {
        NlpTaskConfig.Builder builder = new NlpTaskConfig.Builder()
            .setVocabulary(randomList(10, () -> randomAlphaOfLength(7)))
            .setDoLowerCase(randomBoolean())
            .setWithSpecialTokens(randomBoolean());

        TaskType taskType = randomFrom(TaskType.values());
        builder.setTaskType(taskType);
        if (taskType == TaskType.SENTIMENT_ANALYSIS || taskType == TaskType.NER) {
            if (randomBoolean()) {
                builder.setClassificationLabels(randomList(1, 3, () -> randomAlphaOfLength(7)));
            }
        }

        if (randomBoolean()) {
            builder.setMaxSequenceLength(randomIntBetween(1, 128));
        }

        return builder.build();
    }

    @Override
    protected NlpTaskConfig doParseInstance(XContentParser parser) throws IOException {
        return NlpTaskConfig.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }
}
