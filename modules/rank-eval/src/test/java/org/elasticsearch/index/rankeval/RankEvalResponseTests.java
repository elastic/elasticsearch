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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RankEvalResponseTests extends ESTestCase {

    private static RankEvalResponse createRandomResponse() {
        Map<String, Collection<RatedDocumentKey>> unknownDocs = new HashMap<>();
        int numberOfSets = randomIntBetween(0, 5);
        for (int i = 0; i < numberOfSets; i++) {
            List<RatedDocumentKey> ids = new ArrayList<>();
            int numberOfUnknownDocs = randomIntBetween(0, 5);
            for (int d = 0; d < numberOfUnknownDocs; d++) {
                ids.add(new RatedDocumentKey(randomAsciiOfLength(5), randomAsciiOfLength(5), randomAsciiOfLength(5)));
            }
            unknownDocs.put(randomAsciiOfLength(5), ids);
        }
        return new RankEvalResponse(randomDouble(), unknownDocs );
    }

    public void testSerialization() throws IOException {
        RankEvalResponse randomResponse = createRandomResponse();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            randomResponse.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                RankEvalResponse deserializedResponse = new RankEvalResponse();
                deserializedResponse.readFrom(in);
                assertEquals(randomResponse, deserializedResponse);
                assertEquals(randomResponse.hashCode(), deserializedResponse.hashCode());
                assertNotSame(randomResponse, deserializedResponse);
                assertEquals(-1, in.read());
            }
        }
    }

    public void testToXContent() throws IOException {
        RankEvalResponse randomResponse = createRandomResponse();
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        if (ESTestCase.randomBoolean()) {
            builder.prettyPrint();
        }
        builder.startObject();
        randomResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
    }
}
