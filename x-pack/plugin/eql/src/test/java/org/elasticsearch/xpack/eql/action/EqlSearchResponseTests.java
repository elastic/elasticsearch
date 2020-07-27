/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class EqlSearchResponseTests extends AbstractSerializingTestCase<EqlSearchResponse> {

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
            return hits;
        }
        return null;
    }

    @Override
    protected EqlSearchResponse createTestInstance() {
        return randomEqlSearchResponse();
    }

    @Override
    protected Writeable.Reader<EqlSearchResponse> instanceReader() {
        return EqlSearchResponse::new;
    }

    public static EqlSearchResponse randomEqlSearchResponse() {
        TotalHits totalHits = null;
        if (randomBoolean()) {
            totalHits = new TotalHits(randomIntBetween(100, 1000), TotalHits.Relation.EQUAL_TO);
        }
        return createRandomInstance(totalHits);
    }

    public static EqlSearchResponse createRandomEventsResponse(TotalHits totalHits) {
        EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new EqlSearchResponse.Hits(randomEvents(), null, null, totalHits);
        }
        if (randomBoolean()) {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
        } else {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(),
                randomAlphaOfLength(10), randomBoolean(), randomBoolean());
        }
    }

    public static EqlSearchResponse createRandomSequencesResponse(TotalHits totalHits) {
        int size = randomIntBetween(1, 10);
        List<EqlSearchResponse.Sequence> seq = null;
        if (randomBoolean()) {
            seq = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                List<String> joins = null;
                if (randomBoolean()) {
                    joins = Arrays.asList(generateRandomStringArray(6, 11, false));
                }
                seq.add(new EqlSearchResponse.Sequence(joins, randomEvents()));
            }
        }
        EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new EqlSearchResponse.Hits(null, seq, null, totalHits);
        }
        if (randomBoolean()) {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
        } else {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(),
                randomAlphaOfLength(10), randomBoolean(), randomBoolean());
        }
    }

    public static EqlSearchResponse createRandomCountResponse(TotalHits totalHits) {
        int size = randomIntBetween(1, 10);
        List<EqlSearchResponse.Count> cn = null;
        if (randomBoolean()) {
            cn = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                List<String> keys = null;
                if (randomBoolean()) {
                    keys = Arrays.asList(generateRandomStringArray(6, 11, false));
                }
                cn.add(new EqlSearchResponse.Count(randomIntBetween(0, 41), keys, randomFloat()));
            }
        }
        EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new EqlSearchResponse.Hits(null, null, cn, totalHits);
        }
        if (randomBoolean()) {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
        } else {
            return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean(),
                randomAlphaOfLength(10), randomBoolean(), randomBoolean());
        }
    }

    public static EqlSearchResponse createRandomInstance(TotalHits totalHits) {
        int type = between(0, 2);
        switch(type) {
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
    protected EqlSearchResponse doParseInstance(XContentParser parser) {
        return EqlSearchResponse.fromXContent(parser);
    }
}
