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
package org.elasticsearch.client.license;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

public class GetTrialStatusResponseTests extends
    AbstractResponseTestCase<org.elasticsearch.license.GetTrialStatusResponse, GetTrialStatusResponse> {

    @Override
    protected org.elasticsearch.license.GetTrialStatusResponse createServerTestInstance(XContentType xContentType) {
        return new org.elasticsearch.license.GetTrialStatusResponse(randomBoolean());
    }

    @Override
    protected GetTrialStatusResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetTrialStatusResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.license.GetTrialStatusResponse serverTestInstance,
                                   GetTrialStatusResponse clientInstance) {
        org.elasticsearch.license.GetTrialStatusResponse serverInstance =
            new org.elasticsearch.license.GetTrialStatusResponse(clientInstance.isEligibleToStartTrial());
        assertEquals(serverInstance, serverTestInstance);
    }
}
