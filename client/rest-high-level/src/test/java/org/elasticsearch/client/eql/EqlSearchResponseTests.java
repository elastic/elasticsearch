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

package org.elasticsearch.client.eql;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EqlSearchResponseTests extends AbstractResponseTestCase<org.elasticsearch.xpack.eql.action.EqlSearchResponse,
    EqlSearchResponse> {

    static List<SearchHit> randomEvents() {
        int size = randomIntBetween(1, 10);
        List<SearchHit> hits = null;
        if (randomBoolean()) {
            hits = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                hits.add(new SearchHit(i, randomAlphaOfLength(10), new HashMap<>(), new HashMap<>()));
            }
        }
        if (randomBoolean()) {
            return null;
        }
        return hits;
    }

    public static org.elasticsearch.xpack.eql.action.EqlSearchResponse createRandomEventsResponse(TotalHits totalHits) {
        org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits(randomEvents(), null, null, totalHits);
        }
        return new org.elasticsearch.xpack.eql.action.EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
    }

    public static org.elasticsearch.xpack.eql.action.EqlSearchResponse createRandomSequencesResponse(TotalHits totalHits) {
        int size = randomIntBetween(1, 10);
        List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence> seq = null;
        if (randomBoolean()) {
            seq = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                List<String> joins = null;
                if (randomBoolean()) {
                    joins = Arrays.asList(generateRandomStringArray(6, 11, false));
                }
                seq.add(new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence(joins, randomEvents()));
            }
        }
        org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits(null, seq, null, totalHits);
        }
        return new org.elasticsearch.xpack.eql.action.EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
    }

    public static org.elasticsearch.xpack.eql.action.EqlSearchResponse createRandomCountResponse(TotalHits totalHits) {
        int size = randomIntBetween(1, 10);
        List<org.elasticsearch.xpack.eql.action.EqlSearchResponse.Count> cn = null;
        if (randomBoolean()) {
            cn = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                List<String> keys = null;
                if (randomBoolean()) {
                    keys = Arrays.asList(generateRandomStringArray(6, 11, false));
                }
                cn.add(new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Count(randomIntBetween(0, 41), keys, randomFloat()));
            }
        }
        org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits(null, null, cn, totalHits);
        }
        return new org.elasticsearch.xpack.eql.action.EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
    }

    public static org.elasticsearch.xpack.eql.action.EqlSearchResponse createRandomInstance(TotalHits totalHits) {
        int type = between(0, 2);
        switch (type) {
            case 0:
                return createRandomEventsResponse(totalHits);
            case 1:
                return createRandomSequencesResponse(totalHits);
            case 2:
                return createRandomCountResponse(totalHits);
            default:
                return null;
        }
    }

    @Override
    protected org.elasticsearch.xpack.eql.action.EqlSearchResponse createServerTestInstance(XContentType xContentType) {
        TotalHits totalHits = null;
        if (randomBoolean()) {
            totalHits = new TotalHits(randomIntBetween(100, 1000), TotalHits.Relation.EQUAL_TO);
        }
        return createRandomInstance(totalHits);
    }

    @Override
    protected EqlSearchResponse doParseToClientInstance(XContentParser parser) throws IOException {
        return EqlSearchResponse.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.eql.action.EqlSearchResponse serverTestInstance, EqlSearchResponse clientInstance) {
        assertThat(serverTestInstance.took(), is(clientInstance.took()));
        assertThat(serverTestInstance.isTimeout(), is(clientInstance.isTimeout()));
        assertThat(serverTestInstance.hits().totalHits(), is(clientInstance.hits().totalHits()));
        if (serverTestInstance.hits().counts() == null) {
            assertNull(clientInstance.hits().counts());
        } else {
            assertThat(serverTestInstance.hits().counts().size(), equalTo(clientInstance.hits().counts().size()));
            for (int i = 0; i < serverTestInstance.hits().counts().size(); i++) {
                assertThat(serverTestInstance.hits().counts().get(i).count(), is(clientInstance.hits().counts().get(i).count()));
                assertThat(serverTestInstance.hits().counts().get(i).keys(), is(clientInstance.hits().counts().get(i).keys()));
                assertThat(serverTestInstance.hits().counts().get(i).percent(), is(clientInstance.hits().counts().get(i).percent()));
            }
        }
        if (serverTestInstance.hits().events() == null) {
            assertNull(clientInstance.hits().events());
        } else {
            assertThat(serverTestInstance.hits().events().size(), equalTo(clientInstance.hits().events().size()));
            for (int i = 0; i < serverTestInstance.hits().events().size(); i++) {
                assertThat(serverTestInstance.hits().events().get(i), is(clientInstance.hits().events().get(i)));
            }
        }
        if (serverTestInstance.hits().sequences() == null) {
            assertNull(clientInstance.hits().sequences());
        } else {
            assertThat(serverTestInstance.hits().sequences().size(), equalTo(clientInstance.hits().sequences().size()));
            for (int i = 0; i < serverTestInstance.hits().sequences().size(); i++) {
                assertThat(serverTestInstance.hits().sequences().get(i).joinKeys(),
                    is(clientInstance.hits().sequences().get(i).joinKeys()));
                assertThat(serverTestInstance.hits().sequences().get(i).events(), is(clientInstance.hits().sequences().get(i).events()));
            }
        }
    }
}
