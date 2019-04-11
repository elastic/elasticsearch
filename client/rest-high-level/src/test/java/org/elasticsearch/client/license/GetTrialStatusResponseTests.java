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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.client.AbstractHlrcStreamableXContentTestCase;

public class GetTrialStatusResponseTests extends
    AbstractHlrcStreamableXContentTestCase<org.elasticsearch.license.GetTrialStatusResponse, GetTrialStatusResponse> {

    @Override
    public GetTrialStatusResponse doHlrcParseInstance(XContentParser parser) {
        return GetTrialStatusResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.license.GetTrialStatusResponse convertHlrcToInternal(GetTrialStatusResponse instance) {
        return new org.elasticsearch.license.GetTrialStatusResponse(instance.isEligibleToStartTrial());
    }

    @Override
    protected org.elasticsearch.license.GetTrialStatusResponse createBlankInstance() {
        return new org.elasticsearch.license.GetTrialStatusResponse(false);
    }

    @Override
    protected org.elasticsearch.license.GetTrialStatusResponse createTestInstance() {
        return new org.elasticsearch.license.GetTrialStatusResponse(randomBoolean());
    }
}
