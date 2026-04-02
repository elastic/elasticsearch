/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;

public class ClientHitTests extends ESTestCase {

    /** Verifies that all getters pass through values from the underlying SearchHit. */
    public void testGettersUseDelegateValues() {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String docId = randomAlphaOfLengthBetween(1, 20);
        int shardId = randomIntBetween(0, 10);
        long version = randomLongBetween(1, Long.MAX_VALUE);
        long seqNo = randomLongBetween(0, Long.MAX_VALUE);
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        String fieldValue = randomAlphaOfLengthBetween(1, 20);
        BytesReference source = new BytesArray("{\"foo\":\"" + fieldValue + "\"}");
        String routingValue = randomAlphaOfLengthBetween(1, 20);
        long sortLong = randomLong();
        String sortString = randomAlphaOfLengthBetween(1, 20);
        Object[] sortValues = new Object[] { sortLong, sortString };

        SearchHit searchHit = SearchHit.unpooled(randomInt(), docId);
        searchHit.shard(
            new SearchShardTarget(randomAlphaOfLength(10), new ShardId(new Index(indexName, randomAlphaOfLength(10)), shardId), null)
        );
        searchHit.version(version);
        searchHit.setSeqNo(seqNo);
        searchHit.setPrimaryTerm(primaryTerm);
        searchHit.sourceRef(source);
        searchHit.addDocumentFields(
            Map.of(),
            Map.of(RoutingFieldMapper.NAME, new DocumentField(RoutingFieldMapper.NAME, List.of(routingValue)))
        );
        searchHit.sortValues(sortValues, new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW });

        ClientHit clientHit = new ClientHit(searchHit);

        assertThat(clientHit.getIndex(), equalTo(indexName));
        assertThat(clientHit.getId(), equalTo(docId));
        assertThat(clientHit.getVersion(), equalTo(version));
        assertThat(clientHit.getSeqNo(), equalTo(seqNo));
        assertThat(clientHit.getPrimaryTerm(), equalTo(primaryTerm));
        assertThat(clientHit.getSource(), equalTo(source));
        assertThat(clientHit.getXContentType(), equalTo(XContentType.JSON));
        assertThat(clientHit.getRouting(), equalTo(routingValue));
        assertThat(clientHit.getSortValues(), arrayContaining(sortLong, sortString));
    }

    /** Verifies that source and XContentType are null when the SearchHit has no source. */
    public void testReturnsNullSourceWhenSearchHitHasNoSource() {
        SearchHit searchHit = SearchHit.unpooled(randomInt(), randomAlphaOfLengthBetween(1, 20));
        ClientHit clientHit = new ClientHit(searchHit);
        assertNull(clientHit.getSource());
        assertNull(clientHit.getXContentType());
    }

    /** Verifies that routing is null when the _routing field is not present. */
    public void testReturnsNullRoutingWhenNotPresent() {
        SearchHit searchHit = SearchHit.unpooled(randomInt(), randomAlphaOfLengthBetween(1, 20));
        ClientHit clientHit = new ClientHit(searchHit);
        assertNull(clientHit.getRouting());
    }

    /** Verifies that the delegate SearchHit is unpooled before wrapping. */
    public void testUnpoolsSearchHit() {
        String indexName = randomAlphaOfLengthBetween(1, 20);
        String docId = randomAlphaOfLengthBetween(1, 20);
        BytesReference source = new BytesArray("{}");
        SearchHit searchHit = SearchHit.unpooled(randomInt(), docId);
        searchHit.sourceRef(source);
        searchHit.shard(
            new SearchShardTarget(
                randomAlphaOfLength(10),
                new ShardId(new Index(indexName, randomAlphaOfLength(10)), randomIntBetween(0, 10)),
                null
            )
        );

        ClientHit clientHit = new ClientHit(searchHit);

        assertThat(clientHit.getIndex(), equalTo(indexName));
        assertThat(clientHit.getId(), equalTo(docId));
        assertThat(clientHit.getSource(), equalTo(source));
    }

    /** Verifies that unassigned seq_no and primary_term are returned when not set. */
    public void testDefaultSeqNoAndPrimaryTermWhenUnset() {
        SearchHit searchHit = SearchHit.unpooled(randomInt(), randomAlphaOfLengthBetween(1, 20));
        ClientHit clientHit = new ClientHit(searchHit);
        assertThat(clientHit.getSeqNo(), equalTo(SequenceNumbers.UNASSIGNED_SEQ_NO));
        assertThat(clientHit.getPrimaryTerm(), equalTo(SequenceNumbers.UNASSIGNED_PRIMARY_TERM));
    }
}
