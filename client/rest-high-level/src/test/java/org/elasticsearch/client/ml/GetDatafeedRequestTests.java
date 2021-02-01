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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetDatafeedRequestTests extends AbstractXContentTestCase<GetDatafeedRequest> {

    public void testAllDatafeedRequest() {
        GetDatafeedRequest request = GetDatafeedRequest.getAllDatafeedsRequest();

        assertEquals(request.getDatafeedIds().size(), 1);
        assertEquals(request.getDatafeedIds().get(0), "_all");
    }

    public void testNewWithDatafeedId() {
        Exception exception = expectThrows(NullPointerException.class, () -> new GetDatafeedRequest("feed",null));
        assertEquals(exception.getMessage(), "datafeedIds must not contain null values");
    }

    @Override
    protected GetDatafeedRequest createTestInstance() {
        int count = randomIntBetween(0, 10);
        List<String> datafeedIds = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            datafeedIds.add(DatafeedConfigTests.randomValidDatafeedId());
        }

        GetDatafeedRequest request = new GetDatafeedRequest(datafeedIds);

        if (randomBoolean()) {
            request.setAllowNoMatch(randomBoolean());
        }

        return request;
    }

    @Override
    protected GetDatafeedRequest doParseInstance(XContentParser parser) throws IOException {
        return GetDatafeedRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
