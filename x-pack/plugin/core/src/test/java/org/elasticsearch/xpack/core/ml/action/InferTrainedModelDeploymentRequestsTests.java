/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.EmptyConfigUpdateTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigUpdateTests;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InferTrainedModelDeploymentRequestsTests extends AbstractBWCWireSerializationTestCase<
    InferTrainedModelDeploymentAction.Request> {

    private static InferenceConfigUpdate randomInferenceConfigUpdate() {
        return randomFrom(ZeroShotClassificationConfigUpdateTests.createRandom(), EmptyConfigUpdateTests.testInstance());
    }

    @Override
    protected Writeable.Reader<InferTrainedModelDeploymentAction.Request> instanceReader() {
        return InferTrainedModelDeploymentAction.Request::new;
    }

    @Override
    protected InferTrainedModelDeploymentAction.Request createTestInstance() {
        boolean createQueryStringRequest = randomBoolean();

        if (createQueryStringRequest) {
            return new InferTrainedModelDeploymentAction.Request(
                randomAlphaOfLength(4),
                randomBoolean() ? null : randomInferenceConfigUpdate(),
                randomAlphaOfLength(6),
                randomBoolean() ? null : TimeValue.parseTimeValue(randomTimeValue(), "timeout")
            );
        } else {
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

    @Override
    protected InferTrainedModelDeploymentAction.Request mutateInstanceForVersion(
        InferTrainedModelDeploymentAction.Request instance,
        Version version
    ) {
        if (version.before(Version.V_8_6_0)) {
            instance = new InferTrainedModelDeploymentAction.Request(
                instance.getDeploymentId(),
                instance.getUpdate(),
                instance.getDocs(),
                null, // remove textInput
                instance.getInferenceTimeout()
            );
        }

        if (version.before(Version.V_8_3_0)) {
            instance.setSkipQueue(false);
        }

        return instance;
    }
}
