/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.function.Predicate;

public class PhaseExecutionInfoTests extends AbstractXContentTestCase<PhaseExecutionInfo> {

    static PhaseExecutionInfo randomPhaseExecutionInfo(String phaseName) {
        return new PhaseExecutionInfo(randomAlphaOfLength(5), PhaseTests.randomPhase(phaseName),
            randomNonNegativeLong(), randomNonNegativeLong());
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
    protected PhaseExecutionInfo doParseInstance(XContentParser parser) throws IOException {
        return PhaseExecutionInfo.parse(parser, phaseName);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // actions are plucked from the named registry, and it fails if the action is not in the named registry
        return (field) -> field.equals("phase_definition.actions");
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(CollectionUtils.appendToCopy(ClusterModule.getNamedXWriteables(),
                new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse)));
    }
}
