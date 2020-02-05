/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        return new NamedXContentRegistry(entries);
    }
}
