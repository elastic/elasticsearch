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

public class GetBasicStatusResponseTests
    extends AbstractHlrcStreamableXContentTestCase<org.elasticsearch.license.GetBasicStatusResponse, GetBasicStatusResponse> {
    @Override
    public GetBasicStatusResponse doHlrcParseInstance(XContentParser parser) {
        return GetBasicStatusResponse.fromXContent(parser);
    }

    @Override
    public org.elasticsearch.license.GetBasicStatusResponse convertHlrcToInternal(GetBasicStatusResponse instance) {
        return new org.elasticsearch.license.GetBasicStatusResponse(instance.isEligibleToStartBasic());
    }

    @Override
    protected org.elasticsearch.license.GetBasicStatusResponse createBlankInstance() {
        return new org.elasticsearch.license.GetBasicStatusResponse(false);
    }

    @Override
    protected org.elasticsearch.license.GetBasicStatusResponse createTestInstance() {
        return new org.elasticsearch.license.GetBasicStatusResponse(randomBoolean());
    }
}
