/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.persistence;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class SeqNoPrimaryTermAndIndexTests extends ESTestCase {

    public void testEquals() {
        for (int i = 0; i < 30; i++) {
            long seqNo = randomLongBetween(-2, 10_000);
            long primaryTerm = randomLongBetween(-2, 10_000);
            String index = randomAlphaOfLength(10);
            SeqNoPrimaryTermAndIndex first = new SeqNoPrimaryTermAndIndex(seqNo, primaryTerm, index);
            SeqNoPrimaryTermAndIndex second = new SeqNoPrimaryTermAndIndex(seqNo, primaryTerm, index);
            assertThat(first, equalTo(second));
        }
    }

    public void testFromSearchHit() {
        SearchHit searchHit = new SearchHit(1);
        long seqNo = randomLongBetween(-2, 10_000);
        long primaryTerm = randomLongBetween(-2, 10_000);
        String index = randomAlphaOfLength(10);
        searchHit.setSeqNo(seqNo);
        searchHit.setPrimaryTerm(primaryTerm);
        searchHit.shard(new SearchShardTarget("anynode", new ShardId(index, randomAlphaOfLength(10), 1), null, null));
        assertThat(SeqNoPrimaryTermAndIndex.fromSearchHit(searchHit), equalTo(new SeqNoPrimaryTermAndIndex(seqNo, primaryTerm, index)));
    }

    public void testFromIndexResponse() {
        long seqNo = randomLongBetween(-2, 10_000);
        long primaryTerm = randomLongBetween(-2, 10_000);
        String index = randomAlphaOfLength(10);
        IndexResponse indexResponse = new IndexResponse(new ShardId(index, randomAlphaOfLength(10), 1),
            "_doc",
            "asdf",
            seqNo,
            primaryTerm,
            1,
        randomBoolean());

        assertThat(SeqNoPrimaryTermAndIndex.fromIndexResponse(indexResponse),
            equalTo(new SeqNoPrimaryTermAndIndex(seqNo, primaryTerm, index)));
    }
}
