/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction.Request;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;

public class PutTrainedModelVocabularyActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        String modelId = randomAlphaOfLength(10);
        int vocabLength = randomIntBetween(10, 1000);
        return new Request(
            modelId,
            randomList(vocabLength, vocabLength, () -> randomAlphaOfLength(10)),
            randomBoolean() ? null : randomList(1, vocabLength, () -> randomAlphaOfLength(10)),
            randomBoolean() ? null : randomList(vocabLength, vocabLength, ESTestCase::randomDouble)
        );
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
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
