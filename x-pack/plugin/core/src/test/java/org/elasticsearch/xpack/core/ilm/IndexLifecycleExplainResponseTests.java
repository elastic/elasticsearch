/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class IndexLifecycleExplainResponseTests extends AbstractSerializingTestCase<IndexLifecycleExplainResponse> {

    static IndexLifecycleExplainResponse randomIndexExplainResponse() {
        final IndexLifecycleExplainResponse indexLifecycleExplainResponse;
        if (frequently()) {
            indexLifecycleExplainResponse = randomManagedIndexExplainResponse();
        } else {
            indexLifecycleExplainResponse = randomUnmanagedIndexExplainResponse();
        }
        long now = System.currentTimeMillis();
        // So that now is the same for the duration of the test. See #84352
        indexLifecycleExplainResponse.nowSupplier = () -> now;
        return indexLifecycleExplainResponse;
    }

    private static IndexLifecycleExplainResponse randomUnmanagedIndexExplainResponse() {
        return IndexLifecycleExplainResponse.newUnmanagedIndexResponse(randomAlphaOfLength(10));
    }

    private static IndexLifecycleExplainResponse randomManagedIndexExplainResponse() {
        boolean stepNull = randomBoolean();
        return IndexLifecycleExplainResponse.newManagedIndexResponse(
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomLongBetween(0, System.currentTimeMillis()),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomLongBetween(0, System.currentTimeMillis()),
            stepNull ? null : randomAlphaOfLength(10),
            stepNull ? null : randomAlphaOfLength(10),
            stepNull ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            stepNull ? null : randomBoolean(),
            stepNull ? null : randomInt(10),
            stepNull ? null : randomNonNegativeLong(),
            stepNull ? null : randomNonNegativeLong(),
            stepNull ? null : randomNonNegativeLong(),
            stepNull ? null : randomAlphaOfLength(10),
            stepNull ? null : randomAlphaOfLength(10),
            stepNull ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()),
            randomBoolean() ? null : PhaseExecutionInfoTests.randomPhaseExecutionInfo("")
        );
    }

    public void testInvalidStepDetails() {
        final int numNull = randomIntBetween(1, 3);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndexLifecycleExplainResponse.newManagedIndexResponse(
                randomAlphaOfLength(10),
                randomNonNegativeLong(),
                randomAlphaOfLength(10),
                randomBoolean() ? null : randomNonNegativeLong(),
                (numNull == 1) ? null : randomAlphaOfLength(10),
                (numNull == 2) ? null : randomAlphaOfLength(10),
                (numNull == 3) ? null : randomAlphaOfLength(10),
                randomBoolean() ? null : randomAlphaOfLength(10),
                randomBoolean() ? null : randomBoolean(),
                randomBoolean() ? null : randomInt(10),
                randomBoolean() ? null : randomNonNegativeLong(),
                randomBoolean() ? null : randomNonNegativeLong(),
                randomBoolean() ? null : randomNonNegativeLong(),
                randomBoolean() ? null : randomAlphaOfLength(10),
                randomBoolean() ? null : randomAlphaOfLength(10),
                randomBoolean() ? null : randomAlphaOfLength(10),
                randomBoolean() ? null : new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString()),
                randomBoolean() ? null : PhaseExecutionInfoTests.randomPhaseExecutionInfo("")
            )
        );
        assertThat(exception.getMessage(), startsWith("managed index response must have complete step details"));
        assertThat(exception.getMessage(), containsString("=null"));
    }

    public void testIndexAges() {
        IndexLifecycleExplainResponse unmanagedExplainResponse = randomUnmanagedIndexExplainResponse();
        assertThat(unmanagedExplainResponse.getLifecycleDate(), is(nullValue()));
        assertThat(unmanagedExplainResponse.getAge(System::currentTimeMillis), is(TimeValue.MINUS_ONE));

        assertThat(unmanagedExplainResponse.getIndexCreationDate(), is(nullValue()));
        assertThat(unmanagedExplainResponse.getTimeSinceIndexCreation(System::currentTimeMillis), is(nullValue()));

        IndexLifecycleExplainResponse managedExplainResponse = IndexLifecycleExplainResponse.newManagedIndexResponse(
            "indexName",
            12345L,
            "policy",
            5678L,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertThat(managedExplainResponse.getLifecycleDate(), is(notNullValue()));
        Long now = 1_000_000L;
        assertThat(managedExplainResponse.getAge(() -> now), is(notNullValue()));
        assertThat(
            managedExplainResponse.getAge(() -> now),
            is(equalTo(TimeValue.timeValueMillis(now - managedExplainResponse.getLifecycleDate())))
        );
        assertThat(managedExplainResponse.getAge(() -> 0L), is(equalTo(TimeValue.ZERO)));
        assertThat(managedExplainResponse.getIndexCreationDate(), is(notNullValue()));
        assertThat(managedExplainResponse.getTimeSinceIndexCreation(() -> now), is(notNullValue()));
        assertThat(
            managedExplainResponse.getTimeSinceIndexCreation(() -> now),
            is(equalTo(TimeValue.timeValueMillis(now - managedExplainResponse.getIndexCreationDate())))
        );
        assertThat(managedExplainResponse.getTimeSinceIndexCreation(() -> 0L), is(equalTo(TimeValue.ZERO)));
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
        Long indexCreationDate = instance.getIndexCreationDate();
        String policy = instance.getPolicyName();
        String phase = instance.getPhase();
        String action = instance.getAction();
        String step = instance.getStep();
        String failedStep = instance.getFailedStep();
        Boolean isAutoRetryableError = instance.isAutoRetryableError();
        Integer failedStepRetryCount = instance.getFailedStepRetryCount();
        Long policyTime = instance.getLifecycleDate();
        Long phaseTime = instance.getPhaseTime();
        Long actionTime = instance.getActionTime();
        Long stepTime = instance.getStepTime();
        String repositoryName = instance.getRepositoryName();
        String snapshotName = instance.getSnapshotName();
        String shrinkIndexName = instance.getShrinkIndexName();
        boolean managed = instance.managedByILM();
        BytesReference stepInfo = instance.getStepInfo();
        PhaseExecutionInfo phaseExecutionInfo = instance.getPhaseExecutionInfo();
        if (managed) {
            switch (between(0, 14)) {
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
                        stepInfo = randomValueOtherThan(
                            stepInfo,
                            () -> new BytesArray(new RandomStepInfo(() -> randomAlphaOfLength(10)).toString())
                        );
                    } else {
                        stepInfo = null;
                    }
                    break;
                case 9:
                    phaseExecutionInfo = randomValueOtherThan(
                        phaseExecutionInfo,
                        () -> PhaseExecutionInfoTests.randomPhaseExecutionInfo("")
                    );
                    break;
                case 10:
                    return IndexLifecycleExplainResponse.newUnmanagedIndexResponse(index);
                case 11:
                    isAutoRetryableError = true;
                    failedStepRetryCount = randomValueOtherThan(failedStepRetryCount, () -> randomInt(10));
                    break;
                case 12:
                    repositoryName = randomValueOtherThan(repositoryName, () -> randomAlphaOfLengthBetween(5, 10));
                    break;
                case 13:
                    snapshotName = randomValueOtherThan(snapshotName, () -> randomAlphaOfLengthBetween(5, 10));
                    break;
                case 14:
                    shrinkIndexName = randomValueOtherThan(shrinkIndexName, () -> randomAlphaOfLengthBetween(5, 10));
                    break;
                default:
                    throw new AssertionError("Illegal randomisation branch");
            }
            return IndexLifecycleExplainResponse.newManagedIndexResponse(
                index,
                indexCreationDate,
                policy,
                policyTime,
                phase,
                action,
                step,
                failedStep,
                isAutoRetryableError,
                failedStepRetryCount,
                phaseTime,
                actionTime,
                stepTime,
                repositoryName,
                snapshotName,
                shrinkIndexName,
                stepInfo,
                phaseExecutionInfo
            );
        } else {
            return switch (between(0, 1)) {
                case 0 -> IndexLifecycleExplainResponse.newUnmanagedIndexResponse(index + randomAlphaOfLengthBetween(1, 5));
                case 1 -> randomManagedIndexExplainResponse();
                default -> throw new AssertionError("Illegal randomisation branch");
            };
        }
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, MockAction.NAME, MockAction::new))
        );
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            CollectionUtils.appendToCopy(
                ClusterModule.getNamedXWriteables(),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(MockAction.NAME), MockAction::parse)
            )
        );
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
