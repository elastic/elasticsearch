/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;

public class PhaseExecutionInfoTests extends AbstractSerializingTestCase<PhaseExecutionInfo> {

    static PhaseExecutionInfo randomPhaseExecutionInfo(String phaseName) {
        return new PhaseExecutionInfo(
            randomAlphaOfLength(5),
            PhaseTests.randomTestPhase(phaseName),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    String phaseName;

    @Before
    public void setupPhaseName() {
        phaseName = randomAlphaOfLength(7);
    }

    @Override
    protected PhaseExecutionInfo createTestInstance() {
        return randomPhaseExecutionInfo(phaseName);
    }

    @Override
    protected Reader<PhaseExecutionInfo> instanceReader() {
        return PhaseExecutionInfo::new;
    }

    @Override
    protected PhaseExecutionInfo doParseInstance(XContentParser parser) throws IOException {
        return PhaseExecutionInfo.parse(parser, phaseName);
    }

    @Override
    protected PhaseExecutionInfo mutateInstance(PhaseExecutionInfo instance) throws IOException {
        String policyName = instance.getPolicyName();
        Phase phase = instance.getPhase();
        long version = instance.getVersion();
        long modifiedDate = instance.getModifiedDate();
        switch (between(0, 3)) {
            case 0 -> policyName = policyName + randomAlphaOfLengthBetween(1, 5);
            case 1 -> phase = randomValueOtherThan(phase, () -> PhaseTests.randomTestPhase(randomAlphaOfLength(6)));
            case 2 -> version++;
            case 3 -> modifiedDate++;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new PhaseExecutionInfo(policyName, phase, version, modifiedDate);
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
}
