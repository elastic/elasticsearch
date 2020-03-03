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
