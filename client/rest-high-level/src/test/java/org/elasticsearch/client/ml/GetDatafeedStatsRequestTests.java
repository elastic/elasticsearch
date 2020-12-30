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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetDatafeedStatsRequestTests extends AbstractXContentTestCase<GetDatafeedStatsRequest> {

    public void testAllDatafeedsRequest() {
        GetDatafeedStatsRequest request = GetDatafeedStatsRequest.getAllDatafeedStatsRequest();

        assertEquals(request.getDatafeedIds().size(), 1);
        assertEquals(request.getDatafeedIds().get(0), "_all");
    }

    public void testNewWithDatafeedId() {
        Exception exception = expectThrows(NullPointerException.class, () -> new GetDatafeedStatsRequest("datafeed", null));
        assertEquals(exception.getMessage(), "datafeedIds must not contain null values");
    }

    @Override
    protected GetDatafeedStatsRequest createTestInstance() {
        int datafeedCount = randomIntBetween(0, 10);
        List<String> datafeedIds = new ArrayList<>(datafeedCount);

        for (int i = 0; i < datafeedCount; i++) {
            datafeedIds.add(randomAlphaOfLength(10));
        }

        GetDatafeedStatsRequest request = new GetDatafeedStatsRequest(datafeedIds);

        if (randomBoolean()) {
            request.setAllowNoMatch(randomBoolean());
        }

        return request;
    }

    @Override
    protected GetDatafeedStatsRequest doParseInstance(XContentParser parser) throws IOException {
        return GetDatafeedStatsRequest.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
