/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InferTrainedModelDeploymentRequestsTests extends AbstractWireSerializingTestCase<InferTrainedModelDeploymentAction.Request> {

    @Override
    protected Writeable.Reader<InferTrainedModelDeploymentAction.Request> instanceReader() {
        return InferTrainedModelDeploymentAction.Request::new;
    }

    @Override
    protected InferTrainedModelDeploymentAction.Request createTestInstance() {
        boolean createQueryStringRequest = randomBoolean();
        InferTrainedModelDeploymentAction.Request request;

        if (createQueryStringRequest) {
            request = InferTrainedModelDeploymentAction.Request.forTextInput(
                randomAlphaOfLength(4),
                randomBoolean() ? null : InferModelActionRequestTests.randomInferenceConfigUpdate(),
                Arrays.asList(generateRandomStringArray(4, 7, false)),
                randomBoolean() ? null : randomTimeValue()
            );
        } else {
            List<Map<String, Object>> docs = randomList(
                5,
                () -> randomMap(1, 3, () -> Tuple.tuple(randomAlphaOfLength(7), randomAlphaOfLength(7)))
            );

            request = InferTrainedModelDeploymentAction.Request.forDocs(
                randomAlphaOfLength(4),
                randomBoolean() ? null : InferModelActionRequestTests.randomInferenceConfigUpdate(),
                docs,
                randomBoolean() ? null : randomTimeValue()
            );
        }
        request.setHighPriority(randomBoolean());
        request.setPrefixType(randomFrom(TrainedModelPrefixStrings.PrefixType.values()));
        request.setChunkResults(randomBoolean());
        return request;
    }

    @Override
    protected InferTrainedModelDeploymentAction.Request mutateInstance(InferTrainedModelDeploymentAction.Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public void testTimeoutNotNull() {
        assertNotNull(createTestInstance().getInferenceTimeout());
    }

    public void testTimeoutNull() {
        assertNull(createTestInstance().getTimeout());
    }
}
