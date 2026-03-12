/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.reindex.PaginatedHitSource.SearchFailure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class SearchFailureTests extends ESTestCase {

    public void testConstructorWithReasonOnly() {
        Throwable reason = randomException();
        SearchFailure failure = new SearchFailure(reason);
        assertSame(reason, failure.getReason());
        assertNull(failure.getIndex());
        assertNull(failure.getShardId());
        assertNull(failure.getNodeId());
        assertEquals(ExceptionsHelper.status(reason), failure.getStatus());
    }

    public void testConstructorWithAllFields() {
        Throwable reason = randomException();
        String index = randomAlphaOfLengthBetween(3, 10);
        Integer shardId = randomIntBetween(0, 100);
        String nodeId = randomAlphaOfLengthBetween(3, 10);
        SearchFailure failure = new SearchFailure(reason, index, shardId, nodeId);
        assertSame(reason, failure.getReason());
        assertEquals(index, failure.getIndex());
        assertEquals(shardId, failure.getShardId());
        assertEquals(nodeId, failure.getNodeId());
        assertEquals(ExceptionsHelper.status(reason), failure.getStatus());
    }

    public void testToXContentIncludesExpectedFields() {
        String message = randomAlphaOfLengthBetween(1, 20);
        Throwable reason = randomException(message);
        String index = randomAlphaOfLengthBetween(3, 10);
        Integer shardId = randomIntBetween(0, 10);
        String nodeId = randomAlphaOfLengthBetween(3, 10);
        SearchFailure failure = new SearchFailure(reason, index, shardId, nodeId);
        String json = Strings.toString(failure);
        Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
        assertThat(map.get(SearchFailure.INDEX_FIELD), equalTo(index));
        assertThat(map.get(SearchFailure.SHARD_FIELD), equalTo(shardId));
        assertThat(map.get(SearchFailure.NODE_FIELD), equalTo(nodeId));
        assertThat(map.get(SearchFailure.STATUS_FIELD), equalTo(failure.getStatus().getStatus()));
        assertThat(map, hasKey(SearchFailure.REASON_FIELD));
        @SuppressWarnings("unchecked")
        Map<String, Object> reasonMap = (Map<String, Object>) map.get(SearchFailure.REASON_FIELD);
        assertThat(reasonMap.get("type"), notNullValue());
        assertThat(reasonMap.get("reason"), equalTo(message));
    }

    public void testToXContentOmitsNullOptionalFields() {
        SearchFailure failure = new SearchFailure(randomException());
        String json = Strings.toString(failure);
        Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
        assertThat(map, not(hasKey(SearchFailure.INDEX_FIELD)));
        assertThat(map, not(hasKey(SearchFailure.SHARD_FIELD)));
        assertThat(map, not(hasKey(SearchFailure.NODE_FIELD)));
        assertThat(map, hasKey(SearchFailure.STATUS_FIELD));
        assertThat(map, hasKey(SearchFailure.REASON_FIELD));
    }

    public static Throwable randomException() {
        return randomException(randomAlphaOfLengthBetween(1, 20));
    }

    public static Throwable randomException(String message) {
        return randomFrom(new IllegalArgumentException(message), new IllegalStateException(message), new ElasticsearchException(message));
    }
}
