/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.enrich;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyStatus;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ExecutePolicyResponseTests extends AbstractResponseTestCase<ExecuteEnrichPolicyAction.Response, ExecutePolicyResponse> {

    @Override
    protected ExecuteEnrichPolicyAction.Response createServerTestInstance(XContentType xContentType) {
        if (randomBoolean()) {
            return new ExecuteEnrichPolicyAction.Response(new ExecuteEnrichPolicyStatus(randomAlphaOfLength(4)));
        } else {
            return new ExecuteEnrichPolicyAction.Response(new TaskId(randomAlphaOfLength(4), randomNonNegativeLong()));
        }
    }

    @Override
    protected ExecutePolicyResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return ExecutePolicyResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(ExecuteEnrichPolicyAction.Response serverTestInstance, ExecutePolicyResponse clientInstance) {
        if (serverTestInstance.getStatus() != null) {
            assertThat(clientInstance.getExecutionStatus().getPhase(), equalTo(serverTestInstance.getStatus().getPhase()));
            assertThat(clientInstance.getTaskId(), nullValue());
        } else if (serverTestInstance.getTaskId() != null) {
            assertThat(clientInstance.getTaskId(), equalTo(clientInstance.getTaskId()));
            assertThat(clientInstance.getExecutionStatus(), nullValue());
        } else {
            assert false;
        }
    }
}
