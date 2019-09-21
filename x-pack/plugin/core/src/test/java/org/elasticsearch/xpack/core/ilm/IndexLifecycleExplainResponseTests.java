/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.startsWith;

public class IndexLifecycleExplainResponseTests extends AbstractSerializingTestCase<IndexLifecycleExplainResponse> {

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
        boolean stepNull = randomBoolean();
        return IndexLifecycleExplainResponse.newManagedIndexResponse(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomNonNegativeLong(),
            stepNull ? null : randomAlphaOfLength(10),
            stepNull ? null : randomAlphaOfLength(10),
            stepNull ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            stepNull ? null : randomNonNegativeLong(),
            stepNull ? null : randomNonNegativeLong(),
            stepNull ? null : randomNonNegativeLong(),
            randomBoolean() ? null : new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()),
            randomBoolean() ? null : PhaseExecutionInfoTests.randomPhaseExecutionInfo(""));
    }

    public void testInvalidStepDetails() {
        final int numNull = randomIntBetween(1, 3);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            IndexLifecycleExplainResponse.newManagedIndexResponse(randomAlphaOfLength(10),
                randomAlphaOfLength(10),
                randomBoolean() ? null : randomNonNegativeLong(),
                (numNull == 1) ? null : randomAlphaOfLength(10),
                (numNull == 2) ? null : randomAlphaOfLength(10),
                (numNull == 3) ? null : randomAlphaOfLength(10),
                randomBoolean() ? null : randomAlphaOfLength(10),
                randomBoolean() ? null : randomNonNegativeLong(),
                randomBoolean() ? null : randomNonNegativeLong(),
                randomBoolean() ? null : randomNonNegativeLong(),
                randomBoolean() ? null : new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()),
                randomBoolean() ? null : PhaseExecutionInfoTests.randomPhaseExecutionInfo("")));
        assertThat(exception.getMessage(), startsWith("managed index response must have complete step details"));
        assertThat(exception.getMessage(), containsString("=null"));
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
    protected boolean assertToXContentEquivalence() {
        return false;
    }

    @Override
    protected IndexLifecycleExplainResponse mutateInstance(IndexLifecycleExplainResponse instance) throws IOException {
        String index = instance.getIndex();
        String policy = instance.getPolicyName();
        String phase = instance.getPhase();
        String action = instance.getAction();
        String step = instance.getStep();
        String failedStep = instance.getFailedStep();
        Long policyTime = instance.getLifecycleDate();
        Long phaseTime = instance.getPhaseTime();
        Long actionTime = instance.getActionTime();
        Long stepTime = instance.getStepTime();
        boolean managed = instance.managedByILM();
        BytesReference stepInfo = instance.getStepInfo();
        PhaseExecutionInfo phaseExecutionInfo = instance.getPhaseExecutionInfo();
        if (managed) {
            switch (between(0, 10)) {
            case 0:
                index = index + randomAlphaOfLengthBetween(1, 5);
                break;
            case 1:
                policy = policy + randomAlphaOfLengthBetween(1, 5);
                break;
            case 2:
                phase = randomAlphaOfLengthBetween(1, 5);
                action = randomAlphaOfLengthBetween(1, 5);
                step = randomAlphaOfLengthBetween(1, 5);
                break;
            case 3:
                phaseTime = randomValueOtherThan(phaseTime, () -> randomLongBetween(0, 100000));
                break;
            case 4:
                actionTime = randomValueOtherThan(actionTime, () -> randomLongBetween(0, 100000));
                break;
            case 5:
                stepTime = randomValueOtherThan(stepTime, () -> randomLongBetween(0, 100000));
                break;
            case 6:
                if (Strings.hasLength(failedStep) == false) {
                    failedStep = randomAlphaOfLength(10);
                } else if (randomBoolean()) {
                    failedStep = failedStep + randomAlphaOfLengthBetween(1, 5);
                } else {
                    failedStep = null;
                }
                break;
            case 7:
                policyTime = randomValueOtherThan(policyTime, () -> randomLongBetween(0, 100000));
                break;
            case 8:
                if (Strings.hasLength(stepInfo) == false) {
                    stepInfo = new BytesArray(randomByteArrayOfLength(100));
                } else if (randomBoolean()) {
                    stepInfo = randomValueOtherThan(stepInfo,
                            () -> new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()));
                } else {
                    stepInfo = null;
                }
                break;
            case 9:
                phaseExecutionInfo = randomValueOtherThan(phaseExecutionInfo, () -> PhaseExecutionInfoTests.randomPhaseExecutionInfo(""));
                break;
            case 10:
                return IndexLifecycleExplainResponse.newUnmanagedIndexResponse(index);
            default:
                throw new AssertionError("Illegal randomisation branch");
            }
            return IndexLifecycleExplainResponse.newManagedIndexResponse(index, policy, policyTime, phase, action, step, failedStep,
                    phaseTime, actionTime, stepTime, stepInfo, phaseExecutionInfo);
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

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays
            .asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new)));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MockAction.NAME), MockAction::parse));
        return new NamedXContentRegistry(entries);
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
