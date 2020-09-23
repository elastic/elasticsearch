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

package org.elasticsearch.client.transform.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.GetTransformResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction;
import org.elasticsearch.xpack.core.transform.action.GetTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class GetTransformResponseTests extends AbstractResponseTestCase<
    GetTransformAction.Response,
    org.elasticsearch.client.transform.GetTransformResponse> {

    public static Response randomResponse() {
        List<TransformConfig> configs = new ArrayList<>();
        for (int i = 0; i < randomInt(10); ++i) {
            configs.add(TransformConfigTests.randomTransformConfig());
        }

        return new Response(configs, randomNonNegativeLong());
    }

    @Override
    protected Response createServerTestInstance(XContentType xContentType) {
        return randomResponse();
    }

    @Override
    protected GetTransformResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return org.elasticsearch.client.transform.GetTransformResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(Response serverTestInstance, GetTransformResponse clientInstance) {
        assertThat(serverTestInstance.getTransformConfigurations(), equalTo(clientInstance.getTransformConfigurations()));
        assertThat(serverTestInstance.getCount(), equalTo(clientInstance.getCount()));
    }
}
