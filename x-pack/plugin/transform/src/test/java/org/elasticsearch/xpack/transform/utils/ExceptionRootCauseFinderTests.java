/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.utils;

import org.elasticsearch.action.DocWriteRequest.OpType;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.TranslogException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentLocation;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ExceptionRootCauseFinderTests extends ESTestCase {

    public void testGetFirstIrrecoverableExceptionFromBulkResponses() {
        Map<Integer, BulkItemResponse> bulkItemResponses = bulkItemResponses(
            new DocumentParsingException(XContentLocation.UNKNOWN, "document parsing error"),
            new ResourceNotFoundException("resource not found error"),
            new IllegalArgumentException("illegal argument error"),
            new EsRejectedExecutionException("es rejected execution"),
            new TranslogException(new ShardId("the_index", "uid", 0), "translog error"),
            new ElasticsearchSecurityException("Authentication required", RestStatus.UNAUTHORIZED),
            new ElasticsearchSecurityException("current license is non-compliant for [transform]", RestStatus.FORBIDDEN),
            new ElasticsearchSecurityException("overloaded, to many requests", RestStatus.TOO_MANY_REQUESTS),
            new ElasticsearchSecurityException("internal error", RestStatus.INTERNAL_SERVER_ERROR),
            new IndexNotFoundException("some missing index")
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

    private static Map<Integer, BulkItemResponse> bulkItemResponses(Exception... exceptions) {
        var id = new AtomicInteger(1);
        return Arrays.stream(exceptions)
            .map(exception -> new BulkItemResponse.Failure("the_index", "id", exception))
            .map(failure -> BulkItemResponse.failure(id.get(), OpType.INDEX, failure))
            .collect(Collectors.toMap(response -> id.getAndIncrement(), Function.identity()));
    }

    public void testIsIrrecoverable() {
        assertFalse(ExceptionRootCauseFinder.isExceptionIrrecoverable(new MapperException("mappings problem")));
        assertFalse(ExceptionRootCauseFinder.isExceptionIrrecoverable(new TaskCancelledException("cancelled task")));
        assertFalse(
            ExceptionRootCauseFinder.isExceptionIrrecoverable(
                new SearchContextMissingException(new ShardSearchContextId("session-id", 123, null))
            )
        );
        assertFalse(
            ExceptionRootCauseFinder.isExceptionIrrecoverable(
                new CircuitBreakingException("circuit broken", CircuitBreaker.Durability.TRANSIENT)
            )
        );
        assertTrue(ExceptionRootCauseFinder.isExceptionIrrecoverable(new IndexClosedException(new Index("index", "1234"))));
        assertTrue(
            ExceptionRootCauseFinder.isExceptionIrrecoverable(new DocumentParsingException(new XContentLocation(1, 2), "parse error"))
        );
        assertTrue(ExceptionRootCauseFinder.isExceptionIrrecoverable(new IndexNotFoundException("some missing index")));
    }

    private static void assertFirstException(Collection<BulkItemResponse> bulkItemResponses, Class<?> expectedClass, String message) {
        Throwable t = ExceptionRootCauseFinder.getFirstIrrecoverableExceptionFromBulkResponses(bulkItemResponses);
        assertNotNull(t);
        assertEquals(t.getClass(), expectedClass);
        assertEquals(t.getMessage(), message);
    }
}
