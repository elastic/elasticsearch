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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

import static org.elasticsearch.index.reindex.PaginatedSearchFailure.INDEX_FIELD;
import static org.elasticsearch.index.reindex.PaginatedSearchFailure.NODE_FIELD;
import static org.elasticsearch.index.reindex.PaginatedSearchFailure.REASON_FIELD;
import static org.elasticsearch.index.reindex.PaginatedSearchFailure.SHARD_FIELD;
import static org.elasticsearch.index.reindex.PaginatedSearchFailure.STATUS_FIELD;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class PaginatedSearchFailureTests extends ESTestCase {

    public void testConstructorWithReasonOnly() {
        Throwable reason = randomException();
        PaginatedSearchFailure failure = new PaginatedSearchFailure(reason);
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
        PaginatedSearchFailure failure = new PaginatedSearchFailure(reason, index, shardId, nodeId);
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
        PaginatedSearchFailure failure = new PaginatedSearchFailure(reason, index, shardId, nodeId);
        String json = Strings.toString(failure);
        Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
        assertThat(map.get(INDEX_FIELD), equalTo(index));
        assertThat(map.get(SHARD_FIELD), equalTo(shardId));
        assertThat(map.get(NODE_FIELD), equalTo(nodeId));
        assertThat(map.get(STATUS_FIELD), equalTo(failure.getStatus().getStatus()));
        assertThat(map, hasKey(REASON_FIELD));
        @SuppressWarnings("unchecked")
        Map<String, Object> reasonMap = (Map<String, Object>) map.get(REASON_FIELD);
        assertThat(reasonMap.get("type"), notNullValue());
        assertThat(reasonMap.get("reason"), equalTo(message));
    }

    public void testToXContentOmitsNullOptionalFields() {
        PaginatedSearchFailure failure = new PaginatedSearchFailure(randomException());
        String json = Strings.toString(failure);
        Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
        assertThat(map, not(hasKey(INDEX_FIELD)));
        assertThat(map, not(hasKey(SHARD_FIELD)));
        assertThat(map, not(hasKey(NODE_FIELD)));
        assertThat(map, hasKey(STATUS_FIELD));
        assertThat(map, hasKey(REASON_FIELD));
    }

    public static Throwable randomException() {
        return randomException(randomAlphaOfLengthBetween(1, 20));
    }

    public static Throwable randomException(String message) {
        return randomFrom(new IllegalArgumentException(message), new IllegalStateException(message), new ElasticsearchException(message));
    }
}
