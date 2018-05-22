/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.RandomStepInfo;

import java.io.IOException;

public class IndexExplainResponseTests extends AbstractSerializingTestCase<IndexExplainResponse> {

    static IndexExplainResponse randomIndexExplainResponse() {
        return new IndexExplainResponse(randomAlphaOfLength(10), randomAlphaOfLength(10), randomNonNegativeLong(), randomAlphaOfLength(10),
                randomAlphaOfLength(10), randomAlphaOfLength(10), randomBoolean() ? "" : randomAlphaOfLength(10), randomNonNegativeLong(),
                randomNonNegativeLong(), randomNonNegativeLong(), randomBoolean() ? new BytesArray(new byte[0])
                        : new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()));
    }

    @Override
    protected IndexExplainResponse createTestInstance() {
        return randomIndexExplainResponse();
    }

    @Override
    protected Reader<IndexExplainResponse> instanceReader() {
        return IndexExplainResponse::new;
    }

    @Override
    protected IndexExplainResponse doParseInstance(XContentParser parser) throws IOException {
        return IndexExplainResponse.PARSER.apply(parser, null);
    }

    @Override
    protected IndexExplainResponse mutateInstance(IndexExplainResponse instance) throws IOException {
        String index = instance.getIndex();
        String policy = instance.getPolicyName();
        String phase = instance.getPhase();
        String action = instance.getAction();
        String step = instance.getStep();
        String failedStep = instance.getFailedStep();
        long policyTime = instance.getLifecycleDate();
        long phaseTime = instance.getPhaseTime();
        long actionTime = instance.getActionTime();
        long stepTime = instance.getStepTime();
        BytesReference stepInfo = instance.getStepInfo();
        switch (between(0, 10)) {
        case 0:
            index = index + randomAlphaOfLengthBetween(1, 5);
            break;
        case 1:
            policy = policy + randomAlphaOfLengthBetween(1, 5);
            break;
        case 2:
            phase = phase + randomAlphaOfLengthBetween(1, 5);
            break;
        case 3:
            action = action + randomAlphaOfLengthBetween(1, 5);
            break;
        case 4:
            step = step + randomAlphaOfLengthBetween(1, 5);
            break;
        case 5:
            if (Strings.hasLength(failedStep) == false) {
                failedStep = randomAlphaOfLength(10);
            } else if (randomBoolean()) {
                failedStep = failedStep + randomAlphaOfLengthBetween(1, 5);
            } else {
                failedStep = "";
            }
            break;
        case 6:
            policyTime += randomLongBetween(0, 100000);
            break;
        case 7:
            phaseTime += randomLongBetween(0, 100000);
            break;
        case 8:
            actionTime += randomLongBetween(0, 100000);
            break;
        case 9:
            stepTime += randomLongBetween(0, 100000);
            break;
        case 10:
            if (stepInfo.length() == 0) {
                stepInfo = new BytesArray(randomByteArrayOfLength(100));
            } else if (randomBoolean()) {
                stepInfo = randomValueOtherThan(stepInfo,
                        () -> new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()));
            } else {
                stepInfo = new BytesArray(new byte[0]);
            }
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new IndexExplainResponse(index, policy, policyTime, phase, action, step, failedStep, phaseTime, actionTime, stepTime,
                stepInfo);
    }

}
