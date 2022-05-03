/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigUpdateTests;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InferTrainedModelDeploymentRequestsTests extends AbstractWireSerializingTestCase<InferTrainedModelDeploymentAction.Request> {

    private static InferenceConfigUpdate randomInferenceConfigUpdate() {
        return randomFrom(ZeroShotClassificationConfigUpdateTests.createRandom(), EmptyConfigUpdateTests.testInstance());
    }

    @Override
    protected Writeable.Reader<InferTrainedModelDeploymentAction.Request> instanceReader() {
        return InferTrainedModelDeploymentAction.Request::new;
    }

    @Override
    protected InferTrainedModelDeploymentAction.Request createTestInstance() {
        List<Map<String, Object>> docs = randomList(
            5,
            () -> randomMap(1, 3, () -> Tuple.tuple(randomAlphaOfLength(7), randomAlphaOfLength(7)))
        );

        return new InferTrainedModelDeploymentAction.Request(
            randomAlphaOfLength(4),
            randomBoolean() ? null : randomInferenceConfigUpdate(),
            docs,
            randomBoolean() ? null : TimeValue.parseTimeValue(randomTimeValue(), "timeout")
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    public void testTimeoutNotNull() {
        assertNotNull(createTestInstance().getInferenceTimeout());
    }

    public void testTimeoutNull() {
        assertNull(createTestInstance().getTimeout());
    }
}
