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

public class GetBasicStatusResponseTests
    extends AbstractResponseTestCase<org.elasticsearch.license.GetBasicStatusResponse, GetBasicStatusResponse> {

    @Override
    protected org.elasticsearch.license.GetBasicStatusResponse createServerTestInstance(XContentType xContentType) {
        return new org.elasticsearch.license.GetBasicStatusResponse(randomBoolean());
    }

    @Override
    protected GetBasicStatusResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return GetBasicStatusResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(org.elasticsearch.license.GetBasicStatusResponse serverTestInstance,
                                   GetBasicStatusResponse clientInstance) {
        org.elasticsearch.license.GetBasicStatusResponse serverInstance =
            new org.elasticsearch.license.GetBasicStatusResponse(clientInstance.isEligibleToStartBasic());
        assertEquals(serverTestInstance, serverInstance);
    }
}
