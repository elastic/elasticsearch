/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentLocation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ExceptionRootCauseFinderTests extends ESTestCase {

    public void testGetFirstIrrecoverableExceptionFromBulkResponses() {
        Map<Integer, BulkItemResponse> bulkItemResponses = new HashMap<>();

        int id = 1;
        // 1
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure(
                    "the_index",
                    "id",
                    new DocumentParsingException(XContentLocation.UNKNOWN, "document parsing error")
                )
            )
        );
        // 2
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure("the_index", "id", new ResourceNotFoundException("resource not found error"))
            )
        );
        // 3
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure("the_index", "id", new IllegalArgumentException("illegal argument error"))
            )
        );
        // 4 not irrecoverable
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure("the_index", "id", new EsRejectedExecutionException("es rejected execution"))
            )
        );
        // 5 not irrecoverable
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure("the_index", "id", new TranslogException(new ShardId("the_index", "uid", 0), "translog error"))
            )
        );
        // 6
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure(
                    "the_index",
                    "id",
                    new ElasticsearchSecurityException("Authentication required", RestStatus.UNAUTHORIZED)
                )
            )
        );
        // 7
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure(
                    "the_index",
                    "id",
                    new ElasticsearchSecurityException("current license is non-compliant for [transform]", RestStatus.FORBIDDEN)
                )
            )
        );
        // 8 not irrecoverable
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure(
                    "the_index",
                    "id",
                    new ElasticsearchSecurityException("overloaded, to many requests", RestStatus.TOO_MANY_REQUESTS)
                )
            )
        );
        // 9 not irrecoverable
        bulkItemResponses.put(
            id,
            BulkItemResponse.failure(
                id++,
                OpType.INDEX,
                new BulkItemResponse.Failure(
                    "the_index",
                    "id",
                    new ElasticsearchSecurityException("internal error", RestStatus.INTERNAL_SERVER_ERROR)
                )
            )
        );

        assertFirstException(bulkItemResponses.values(), DocumentParsingException.class, "document parsing error");
        bulkItemResponses.remove(1);
        assertFirstException(bulkItemResponses.values(), ResourceNotFoundException.class, "resource not found error");
        bulkItemResponses.remove(2);
        assertFirstException(bulkItemResponses.values(), IllegalArgumentException.class, "illegal argument error");
        bulkItemResponses.remove(3);
        assertFirstException(bulkItemResponses.values(), ElasticsearchSecurityException.class, "Authentication required");
        bulkItemResponses.remove(6);
        assertFirstException(
            bulkItemResponses.values(),
            ElasticsearchSecurityException.class,
            "current license is non-compliant for [transform]"
        );
        bulkItemResponses.remove(7);

        assertNull(ExceptionRootCauseFinder.getFirstIrrecoverableExceptionFromBulkResponses(bulkItemResponses.values()));
    }

    private static void assertFirstException(Collection<BulkItemResponse> bulkItemResponses, Class<?> expectedClass, String message) {
        Throwable t = ExceptionRootCauseFinder.getFirstIrrecoverableExceptionFromBulkResponses(bulkItemResponses);
        assertNotNull(t);
        assertEquals(t.getClass(), expectedClass);
        assertEquals(t.getMessage(), message);
    }

}
