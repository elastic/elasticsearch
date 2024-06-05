/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class TopAnswerEntryTests extends AbstractXContentSerializingTestCase<QuestionAnsweringInferenceResults.TopAnswerEntry> {

    public static QuestionAnsweringInferenceResults.TopAnswerEntry createRandomTopAnswerEntry() {
        return new QuestionAnsweringInferenceResults.TopAnswerEntry(randomAlphaOfLength(10), randomDouble(), randomInt(10), randomInt(400));
    }

    @Override
    protected QuestionAnsweringInferenceResults.TopAnswerEntry doParseInstance(XContentParser parser) throws IOException {
        return QuestionAnsweringInferenceResults.TopAnswerEntry.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<QuestionAnsweringInferenceResults.TopAnswerEntry> instanceReader() {
        return QuestionAnsweringInferenceResults.TopAnswerEntry::fromStream;
    }

    @Override
    protected QuestionAnsweringInferenceResults.TopAnswerEntry createTestInstance() {
        return createRandomTopAnswerEntry();
    }

    @Override
    protected QuestionAnsweringInferenceResults.TopAnswerEntry mutateInstance(QuestionAnsweringInferenceResults.TopAnswerEntry instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
