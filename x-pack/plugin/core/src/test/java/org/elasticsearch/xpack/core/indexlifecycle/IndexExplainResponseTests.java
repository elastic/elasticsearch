/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

public class IndexExplainResponseTests extends AbstractSerializingTestCase<IndexLifecycleExplainResponse> {

    static IndexLifecycleExplainResponse randomIndexExplainResponse() {
        if (frequently()) {
            return randomManagedIndexExplainResponse();
        } else {
            return randomUnmanagedIndexExplainResponse();
        }
    }

    private static IndexLifecycleExplainResponse randomUnmanagedIndexExplainResponse() {
        return IndexLifecycleExplainResponse.newUnmanagedIndexResponse(randomAlphaOfLength(10));
    }

    private static IndexLifecycleExplainResponse randomManagedIndexExplainResponse() {
        return IndexLifecycleExplainResponse.newManagedIndexResponse(randomAlphaOfLength(10), randomAlphaOfLength(10), randomBoolean(),
                randomNonNegativeLong(), randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10),
                randomBoolean() ? null : randomAlphaOfLength(10), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                randomBoolean() ? null : new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()));
    }

    @Override
    protected IndexLifecycleExplainResponse createTestInstance() {
        return randomIndexExplainResponse();
    }

    @Override
    protected Reader<IndexLifecycleExplainResponse> instanceReader() {
        return IndexLifecycleExplainResponse::new;
    }

    @Override
    protected IndexLifecycleExplainResponse doParseInstance(XContentParser parser) throws IOException {
        return IndexLifecycleExplainResponse.PARSER.apply(parser, null);
    }

    @Override
    protected IndexLifecycleExplainResponse mutateInstance(IndexLifecycleExplainResponse instance) throws IOException {
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
        boolean managed = instance.managedByILM();
        boolean skip = instance.skip();
        BytesReference stepInfo = instance.getStepInfo();
        if (managed) {
            switch (between(0, 12)) {
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
                    failedStep = null;
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
                if (Strings.hasLength(stepInfo) == false) {
                    stepInfo = new BytesArray(randomByteArrayOfLength(100));
                } else if (randomBoolean()) {
                    stepInfo = randomValueOtherThan(stepInfo,
                            () -> new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()));
                } else {
                    stepInfo = null;
                }
                break;
            case 11:
                skip = skip == false;
                break;
            case 12:
                return IndexLifecycleExplainResponse.newUnmanagedIndexResponse(index);
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
            return IndexLifecycleExplainResponse.newManagedIndexResponse(index, policy, skip, policyTime, phase, action, step, failedStep,
                    phaseTime, actionTime, stepTime, stepInfo);
        } else {
            switch (between(0, 1)) {
            case 0:
                return IndexLifecycleExplainResponse.newUnmanagedIndexResponse(index + randomAlphaOfLengthBetween(1, 5));
            case 1:
                return randomManagedIndexExplainResponse();
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
        }
    }

    private static class RandomStepInfo implements ToXContentObject {

        private final String key;
        private final String value;

        RandomStepInfo(Supplier<String> randomStringSupplier) {
            this.key = randomStringSupplier.get();
            this.value = randomStringSupplier.get();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(key, value);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            RandomStepInfo other = (RandomStepInfo) obj;
            return Objects.equals(key, other.key) && Objects.equals(value, other.value);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

}
