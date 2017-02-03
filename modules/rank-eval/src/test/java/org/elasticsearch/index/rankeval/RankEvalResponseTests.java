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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RankEvalResponseTests extends ESTestCase {

    private static RankEvalResponse createRandomResponse() {
        int numberOfRequests = randomIntBetween(0, 5);
        Map<String, EvalQueryQuality> partials = new HashMap<>(numberOfRequests);
        for (int i = 0; i < numberOfRequests; i++) {
            String id = randomAsciiOfLengthBetween(3, 10);
            int numberOfUnknownDocs = randomIntBetween(0, 5);
            List<DocumentKey> unknownDocs = new ArrayList<>(numberOfUnknownDocs);
            for (int d = 0; d < numberOfUnknownDocs; d++) {
                unknownDocs.add(DocumentKeyTests.createRandomRatedDocumentKey());
            }
            EvalQueryQuality evalQuality = new EvalQueryQuality(id, randomDoubleBetween(0.0, 1.0, true));
            partials.put(id, evalQuality);
        }
        int numberOfErrors = randomIntBetween(0, 2);
        Map<String, Exception> errors = new HashMap<>(numberOfRequests);
        for (int i = 0; i < numberOfErrors; i++) {
            errors.put(randomAsciiOfLengthBetween(3, 10), new IllegalArgumentException(randomAsciiOfLength(10)));
        }
        return new RankEvalResponse(randomDouble(), partials, errors);
    }

    public void testSerialization() throws IOException {
        RankEvalResponse randomResponse = createRandomResponse();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            randomResponse.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                RankEvalResponse deserializedResponse = new RankEvalResponse();
                deserializedResponse.readFrom(in);
                assertEquals(randomResponse.getQualityLevel(), deserializedResponse.getQualityLevel(), Double.MIN_VALUE);
                assertEquals(randomResponse.getPartialResults(), deserializedResponse.getPartialResults());
                assertEquals(randomResponse.getFailures().keySet(), deserializedResponse.getFailures().keySet());
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
        randomResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        // TODO check the correctness of the output
    }
}
