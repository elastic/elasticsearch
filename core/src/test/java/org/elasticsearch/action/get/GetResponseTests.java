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

package org.elasticsearch.action.get;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.index.get.GetResultTests.copyGetResult;
import static org.elasticsearch.index.get.GetResultTests.mutateGetResult;
import static org.elasticsearch.index.get.GetResultTests.randomGetResult;
import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class GetResponseTests extends ESTestCase {

    public void testToAndFromXContent() throws Exception {
        XContentType xContentType = randomFrom(XContentType.values());
        Tuple<GetResult, GetResult> tuple = randomGetResult(xContentType);
        GetResponse getResponse = new GetResponse(tuple.v1());
        GetResponse expectedGetResponse = new GetResponse(tuple.v2());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toXContent(getResponse, xContentType, humanReadable);
        //test that we can parse what we print out
        GetResponse parsedGetResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedGetResponse = GetResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(expectedGetResponse, parsedGetResponse);
        //print the parsed object out and test that the output is the same as the original output
        BytesReference finalBytes = toXContent(parsedGetResponse, xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
        //check that the source stays unchanged, no shuffling of keys nor anything like that
        assertEquals(expectedGetResponse.getSourceAsString(), parsedGetResponse.getSourceAsString());
    }

    public void testToXContent() throws IOException {
        {
            GetResponse getResponse = new GetResponse(new GetResult("index", "type", "id", 1, true, new BytesArray("{ \"field1\" : " +
                    "\"value1\", \"field2\":\"value2\"}"), Collections.singletonMap("field1", new GetField("field1",
                    Collections.singletonList("value1")))));
            String output = Strings.toString(getResponse);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"_version\":1,\"found\":true,\"_source\":{ \"field1\" " +
                    ": \"value1\", \"field2\":\"value2\"},\"fields\":{\"field1\":[\"value1\"]}}", output);
        }
        {
            GetResponse getResponse = new GetResponse(new GetResult("index", "type", "id", 1, false, null, null));
            String output = Strings.toString(getResponse);
            assertEquals("{\"_index\":\"index\",\"_type\":\"type\",\"_id\":\"id\",\"found\":false}", output);
        }
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(new GetResponse(randomGetResult(XContentType.JSON).v1()), GetResponseTests::copyGetResponse,
                GetResponseTests::mutateGetResponse);
    }
    private static GetResponse copyGetResponse(GetResponse getResponse) {
        return new GetResponse(copyGetResult(getResponse.getResult));
    }

    private static GetResponse mutateGetResponse(GetResponse getResponse) {
        return new GetResponse(mutateGetResult(getResponse.getResult));
    }
}
