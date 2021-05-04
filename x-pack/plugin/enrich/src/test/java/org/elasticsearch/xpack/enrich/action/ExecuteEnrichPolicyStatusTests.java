/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;
import org.elasticsearch.xpack.enrich.EnrichPlugin;

import static org.elasticsearch.xpack.enrich.action.EnrichStatsResponseTests.randomTaskInfo;
import static org.hamcrest.Matchers.equalTo;

public class ExecuteEnrichPolicyStatusTests extends AbstractWireSerializingTestCase<ExecuteEnrichPolicyStatus> {

    @Override
    protected Writeable.Reader<ExecuteEnrichPolicyStatus> instanceReader() {
        return ExecuteEnrichPolicyStatus::new;
    }

    @Override
    protected ExecuteEnrichPolicyStatus createTestInstance() {
        return new ExecuteEnrichPolicyStatus(randomAlphaOfLengthBetween(2, 8));
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        EnrichPlugin enrichPlugin = new EnrichPlugin(Settings.EMPTY);
        return new NamedWriteableRegistry(enrichPlugin.getNamedWriteables());
    }

    public void testEnsureExecuteEnrichPolicyStatusIsRegistered() throws Exception {
        TaskInfo testInstance = randomTaskInfo(createTestInstance());
        TaskInfo instance = copyWriteable(testInstance, getNamedWriteableRegistry(), TaskInfo::new, Version.CURRENT);
        assertThat(testInstance, equalTo(instance));
    }
}
