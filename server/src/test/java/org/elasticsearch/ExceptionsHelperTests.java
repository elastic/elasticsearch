/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.commons.codec.DecoderException;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.Optional;

import static org.elasticsearch.ExceptionsHelper.maybeError;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;

public class ExceptionsHelperTests extends ESTestCase {

    public void testMaybeError() {
        final Error outOfMemoryError = new OutOfMemoryError();
        assertError(outOfMemoryError, outOfMemoryError);

        final DecoderException decoderException = new DecoderException(outOfMemoryError);
        assertError(decoderException, outOfMemoryError);

        final Exception e = new Exception();
        e.addSuppressed(decoderException);
        assertError(e, outOfMemoryError);

        final int depth = randomIntBetween(1, 16);
        Throwable cause = new Exception();
        boolean fatal = false;
        Error error = null;
        for (int i = 0; i < depth; i++) {
            final int length = randomIntBetween(1, 4);
            for (int j = 0; j < length; j++) {
                if (fatal == false && rarely()) {
                    error = new Error();
                    cause.addSuppressed(error);
                    fatal = true;
                } else {
                    cause.addSuppressed(new Exception());
                }
            }
            if (fatal == false && rarely()) {
                cause = error = new Error(cause);
                fatal = true;
            } else {
                cause = new Exception(cause);
            }
        }
        if (fatal) {
            assertError(cause, error);
        } else {
            assertFalse(maybeError(cause).isPresent());
        }

        assertFalse(maybeError(new Exception(new DecoderException())).isPresent());
    }

    private void assertError(final Throwable cause, final Error error) {
        final Optional<Error> maybeError = maybeError(cause);
        assertTrue(maybeError.isPresent());
        assertThat(maybeError.get(), equalTo(error));
    }

    public void testStatus() {
        assertThat(ExceptionsHelper.status(new IllegalArgumentException("illegal")), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ExceptionsHelper.status(new JsonParseException(null, "illegal")), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ExceptionsHelper.status(new EsRejectedExecutionException("rejected")), equalTo(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testGroupBy() {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[]{
            createShardFailureParsingException("error", "node0", "index", 0, null),
            createShardFailureParsingException("error", "node1", "index", 1, null),
            createShardFailureParsingException("error", "node2", "index2", 2, null),
            createShardFailureParsingException("error", "node0", "index", 0, "cluster1"),
            createShardFailureParsingException("error", "node1", "index", 1, "cluster1"),
            createShardFailureParsingException("error", "node2", "index", 2, "cluster1"),
            createShardFailureParsingException("error", "node0", "index", 0, "cluster2"),
            createShardFailureParsingException("error", "node1", "index", 1, "cluster2"),
            createShardFailureParsingException("error", "node2", "index", 2, "cluster2"),
            createShardFailureParsingException("another error", "node2", "index", 2, "cluster2")
        };

        ShardOperationFailedException[] groupBy = ExceptionsHelper.groupBy(failures);
        assertThat(groupBy.length, equalTo(5));
        String[] expectedIndices = new String[]{"index", "index2", "cluster1:index", "cluster2:index", "cluster2:index"};
        String[] expectedErrors = new String[]{"error", "error", "error", "error", "another error"};
        int i = 0;
        for (ShardOperationFailedException shardOperationFailedException : groupBy) {
            assertThat(shardOperationFailedException.getCause().getMessage(), equalTo(expectedErrors[i]));
            assertThat(shardOperationFailedException.index(), equalTo(expectedIndices[i++]));
        }
    }

    private static ShardSearchFailure createShardFailureParsingException(String error, String nodeId,
                                                                         String index, int shardId, String clusterAlias) {
        ParsingException ex = new ParsingException(0, 0, error, new IllegalArgumentException("some bad argument"));
        ex.setIndex(index);
        return new ShardSearchFailure(ex, createSearchShardTarget(nodeId, shardId, index, clusterAlias));
    }

    private static SearchShardTarget createSearchShardTarget(String nodeId, int shardId, String index, String clusterAlias) {
        return new SearchShardTarget(nodeId,
            new ShardId(new Index(index, IndexMetadata.INDEX_UUID_NA_VALUE), shardId), clusterAlias, OriginalIndices.NONE);
    }

    public void testGroupByNullTarget() {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] {
            createShardFailureQueryShardException("error", "index", null),
            createShardFailureQueryShardException("error", "index", null),
            createShardFailureQueryShardException("error", "index", null),
            createShardFailureQueryShardException("error", "index", "cluster1"),
            createShardFailureQueryShardException("error", "index", "cluster1"),
            createShardFailureQueryShardException("error", "index", "cluster1"),
            createShardFailureQueryShardException("error", "index", "cluster2"),
            createShardFailureQueryShardException("error", "index", "cluster2"),
            createShardFailureQueryShardException("error", "index2", null),
            createShardFailureQueryShardException("another error", "index2", null),
        };

        ShardOperationFailedException[] groupBy = ExceptionsHelper.groupBy(failures);
        assertThat(groupBy.length, equalTo(5));
        String[] expectedIndices = new String[]{"index", "cluster1:index", "cluster2:index", "index2", "index2"};
        String[] expectedErrors = new String[]{"error", "error", "error", "error", "another error"};
        int i = 0;
        for (ShardOperationFailedException shardOperationFailedException : groupBy) {
            assertThat(shardOperationFailedException.index(), nullValue());
            assertThat(shardOperationFailedException.getCause(), instanceOf(ElasticsearchException.class));
            ElasticsearchException elasticsearchException = (ElasticsearchException) shardOperationFailedException.getCause();
            assertThat(elasticsearchException.getMessage(), equalTo(expectedErrors[i]));
            assertThat(elasticsearchException.getIndex().getName(), equalTo(expectedIndices[i++]));
        }
    }

    private static ShardSearchFailure createShardFailureQueryShardException(String error, String indexName, String clusterAlias) {
        Index index = new Index(RemoteClusterAware.buildRemoteIndexName(clusterAlias, indexName), "uuid");
        QueryShardException queryShardException = new QueryShardException(index, error, new IllegalArgumentException("parse error"));
        return new ShardSearchFailure(queryShardException, null);
    }

    public void testGroupByNullIndex() {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] {
            new ShardSearchFailure(new IllegalArgumentException("error")),
            new ShardSearchFailure(new ParsingException(0, 0, "error", null)),
        };

        ShardOperationFailedException[] groupBy = ExceptionsHelper.groupBy(failures);
        assertThat(groupBy.length, equalTo(2));
    }

    public void testUnwrapCorruption() {
        final Throwable corruptIndexException = new CorruptIndexException("corrupt", "resource");
        assertThat(ExceptionsHelper.unwrapCorruption(corruptIndexException), equalTo(corruptIndexException));

        final Throwable corruptionAsCause = new RuntimeException(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionAsCause), equalTo(corruptIndexException));

        final Throwable corruptionSuppressed = new RuntimeException();
        corruptionSuppressed.addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionSuppressed), equalTo(corruptIndexException));

        final Throwable corruptionSuppressedOnCause = new RuntimeException(new RuntimeException());
        corruptionSuppressedOnCause.getCause().addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionSuppressedOnCause), equalTo(corruptIndexException));

        final Throwable corruptionCauseOnSuppressed = new RuntimeException();
        corruptionCauseOnSuppressed.addSuppressed(new RuntimeException(corruptIndexException));
        assertThat(ExceptionsHelper.unwrapCorruption(corruptionCauseOnSuppressed), equalTo(corruptIndexException));

        assertThat(ExceptionsHelper.unwrapCorruption(new RuntimeException()), nullValue());
        assertThat(ExceptionsHelper.unwrapCorruption(new RuntimeException(new RuntimeException())), nullValue());

        final Throwable withSuppressedException = new RuntimeException();
        withSuppressedException.addSuppressed(new RuntimeException());
        assertThat(ExceptionsHelper.unwrapCorruption(withSuppressedException), nullValue());
    }

    public void testSuppressedCycle() {
        RuntimeException e1 = new RuntimeException();
        RuntimeException e2 = new RuntimeException();
        e1.addSuppressed(e2);
        e2.addSuppressed(e1);
        ExceptionsHelper.unwrapCorruption(e1);

        final CorruptIndexException corruptIndexException = new CorruptIndexException("corrupt", "resource");
        RuntimeException e3 = new RuntimeException(corruptIndexException);
        e3.addSuppressed(e1);
        assertThat(ExceptionsHelper.unwrapCorruption(e3), equalTo(corruptIndexException));

        RuntimeException e4 = new RuntimeException(e1);
        e4.addSuppressed(corruptIndexException);
        assertThat(ExceptionsHelper.unwrapCorruption(e4), equalTo(corruptIndexException));
    }

    public void testCauseCycle() {
        RuntimeException e1 = new RuntimeException();
        RuntimeException e2 = new RuntimeException(e1);
        e1.initCause(e2);
        ExceptionsHelper.unwrap(e1, IOException.class);
        ExceptionsHelper.unwrapCorruption(e1);
    }
}
