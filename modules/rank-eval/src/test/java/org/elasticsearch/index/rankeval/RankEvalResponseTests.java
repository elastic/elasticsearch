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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RankEvalResponseTests extends ESTestCase {

    private static RankEvalResponse createRandomResponse() {
        int numberOfRequests = randomIntBetween(0, 5);
        Map<String, EvalQueryQuality> partials = new HashMap<>(numberOfRequests);
        for (int i = 0; i < numberOfRequests; i++) {
            String id = randomAlphaOfLengthBetween(3, 10);
            EvalQueryQuality evalQuality = new EvalQueryQuality(id,
                    randomDoubleBetween(0.0, 1.0, true));
            int numberOfDocs = randomIntBetween(0, 5);
            List<RatedSearchHit> ratedHits = new ArrayList<>(numberOfDocs);
            for (int d = 0; d < numberOfDocs; d++) {
                ratedHits.add(searchHit(randomAlphaOfLength(10), randomIntBetween(0, 1000), randomIntBetween(0, 10)));
            }
            evalQuality.addHitsAndRatings(ratedHits);
            partials.put(id, evalQuality);
        }
        int numberOfErrors = randomIntBetween(0, 2);
        Map<String, Exception> errors = new HashMap<>(numberOfRequests);
        for (int i = 0; i < numberOfErrors; i++) {
            errors.put(randomAlphaOfLengthBetween(3, 10),
                    new IllegalArgumentException(randomAlphaOfLength(10)));
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
                assertEquals(randomResponse.getEvaluationResult(), deserializedResponse.getEvaluationResult(), Double.MIN_VALUE);
                assertEquals(randomResponse.getPartialResults(), deserializedResponse.getPartialResults());
                assertEquals(randomResponse.getFailures().keySet(), deserializedResponse.getFailures().keySet());
                assertNotSame(randomResponse, deserializedResponse);
                assertEquals(-1, in.read());
            }
        }
    }

    public void testToXContent() throws IOException {
        EvalQueryQuality coffeeQueryQuality = new EvalQueryQuality("coffee_query", 0.1);
        coffeeQueryQuality.addHitsAndRatings(Arrays.asList(searchHit("index", 123, 5), searchHit("index", 456, null)));
        RankEvalResponse response = new RankEvalResponse(0.123, Collections.singletonMap("coffee_query", coffeeQueryQuality),
                Collections.singletonMap("beer_query", new ParsingException(new XContentLocation(0, 0), "someMsg")));
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        String xContent = response.toXContent(builder, ToXContent.EMPTY_PARAMS).bytes().utf8ToString();
        assertEquals(("{" +
                "    \"quality_level\": 0.123," +
                "    \"details\": {" +
                "        \"coffee_query\": {" +
                "            \"quality_level\": 0.1," +
                "            \"unknown_docs\": [{\"_index\":\"index\",\"_id\":\"456\"}]," +
                "            \"hits\":[{\"hit\":{\"_index\":\"index\",\"_type\":\"\",\"_id\":\"123\",\"_score\":1.0}," +
                "                       \"rating\":5}," +
                "                      {\"hit\":{\"_index\":\"index\",\"_type\":\"\",\"_id\":\"456\",\"_score\":1.0}," +
                "                       \"rating\":null}" +
                "                     ]" +
                "        }" +
                "    }," +
                "    \"failures\": {" +
                "        \"beer_query\": {" +
                "            \"error\": \"ParsingException[someMsg]\"" +
                "        }" +
                "    }" +
                "}").replaceAll("\\s+", ""), xContent);
    }

    private static RatedSearchHit searchHit(String index, int docId, Integer rating) {
        SearchHit hit = new SearchHit(docId, docId + "", new Text(""), Collections.emptyMap());
        hit.shard(new SearchShardTarget("testnode", new Index(index, "uuid"), 0, null));
        hit.score(1.0f);
        return new RatedSearchHit(hit, rating != null ? Optional.of(rating) : Optional.empty());
    }
}
