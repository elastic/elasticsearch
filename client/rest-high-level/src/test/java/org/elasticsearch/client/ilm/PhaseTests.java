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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class PhaseTests extends AbstractXContentTestCase<Phase> {
    private String phaseName;

    @Before
    public void setup() {
        phaseName = randomAlphaOfLength(20);
    }

    @Override
    protected Phase createTestInstance() {
        return randomPhase(phaseName);
    }

    static Phase randomPhase(String phaseName) {
        TimeValue after = null;
        if (randomBoolean()) {
            after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
        }
        Map<String, LifecycleAction> actions = Collections.emptyMap();
        if (randomBoolean()) {
            actions = Collections.singletonMap(DeleteAction.NAME, new DeleteAction());
        }
        return new Phase(phaseName, after, actions);
    }

    @Override
    protected Phase doParseInstance(XContentParser parser) {
        return Phase.parse(parser, phaseName);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // actions are plucked from the named registry, and it fails if the action is not in the named registry
        return (field) -> field.equals("actions");
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedXWriteables());
        entries.add(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        return new NamedXContentRegistry(entries);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testDefaultAfter() {
        Phase phase = new Phase(randomAlphaOfLength(20), null, Collections.emptyMap());
        assertEquals(TimeValue.ZERO, phase.getMinimumAge());
    }
}
