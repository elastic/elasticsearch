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

import java.util.HashMap;

public class EqlSearchResponseTests extends AbstractSerializingTestCase<EqlSearchResponse> {

    static EqlSearchResponse.Events randomEvents() {
        int size = randomIntBetween(1, 10);
        SearchHit[] hits = null;
        if (randomBoolean()) {
            hits = new SearchHit[size];
            for (int i = 0; i < size; i++) {
                hits[i] = new SearchHit(i, randomAlphaOfLength(10), new HashMap<>());
            }
        }
        if (randomBoolean()) {
            return new EqlSearchResponse.Events(hits);
        }
        return null;
    }

    @Override
    protected EqlSearchResponse createTestInstance() {
        TotalHits totalHits = null;
        if (randomBoolean()) {
            totalHits = new TotalHits(randomIntBetween(100, 1000), TotalHits.Relation.EQUAL_TO);
        }
        return createRandomInstance(totalHits);
    }

    @Override
    protected Writeable.Reader<EqlSearchResponse> instanceReader() {
        return EqlSearchResponse::new;
    }

    public static EqlSearchResponse createRandomEventsResponse(TotalHits totalHits) {
        EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new EqlSearchResponse.Hits(randomEvents(), totalHits);
        }
        return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
    }

    public static EqlSearchResponse createRandomSequencesResponse(TotalHits totalHits) {
        int size = randomIntBetween(1, 10);
        EqlSearchResponse.Sequence[] seq = null;
        if (randomBoolean()) {
            seq = new EqlSearchResponse.Sequence[size];
            for (int i = 0; i < size; i++) {
                String[] joins = null;
                if (randomBoolean()) {
                    joins = generateRandomStringArray(6, 11, false);
                }
                seq[i] = new EqlSearchResponse.Sequence(joins, randomEvents());
            }
        }
        EqlSearchResponse.Sequences sequences = null;
        if (randomBoolean()) {
            sequences = new EqlSearchResponse.Sequences(seq);
        }
        EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new EqlSearchResponse.Hits(sequences, totalHits);
        }
        return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
    }

    public static EqlSearchResponse createRandomCountResponse(TotalHits totalHits) {
        int size = randomIntBetween(1, 10);
        EqlSearchResponse.Count[] cn = null;
        if (randomBoolean()) {
            cn = new EqlSearchResponse.Count[size];
            for (int i = 0; i < size; i++) {
                String[] keys = null;
                if (randomBoolean()) {
                    keys = generateRandomStringArray(6, 11, false);
                }
                cn[i] = new EqlSearchResponse.Count(randomIntBetween(0, 41), keys, randomFloat());
            }
        }
        EqlSearchResponse.Counts counts = null;
        if (randomBoolean()) {
            counts = new EqlSearchResponse.Counts(cn);
        }
        EqlSearchResponse.Hits hits = null;
        if (randomBoolean()) {
            hits = new EqlSearchResponse.Hits(counts, totalHits);
        }
        return new EqlSearchResponse(hits, randomIntBetween(0, 1001), randomBoolean());
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
