/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exception;

import org.apache.commons.codec.DecoderException;
import org.apache.lucene.index.CorruptIndexException;
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
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;
import java.util.Optional;

import static org.elasticsearch.exception.ExceptionsHelper.maybeError;
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
        assertThat(ExceptionsHelper.status(new XContentParseException(null, "illegal")), equalTo(RestStatus.BAD_REQUEST));
        assertThat(ExceptionsHelper.status(new EsRejectedExecutionException("rejected")), equalTo(RestStatus.TOO_MANY_REQUESTS));
    }

    public void testGroupBy() {
        ShardOperationFailedException[] failures = new ShardOperationFailedException[] {
            createShardFailureParsingException("error", "node0", "index", 0, null),
            createShardFailureParsingException("error", "node1", "index", 1, null),
            createShardFailureParsingException("error", "node2", "index2", 2, null),
            createShardFailureParsingException("error", "node0", "index", 0, "cluster1"),
            createShardFailureParsingException("error", "node1", "index", 1, "cluster1"),
            createShardFailureParsingException("error", "node2", "index", 2, "cluster1"),
            createShardFailureParsingException("error", "node0", "index", 0, "cluster2"),
            createShardFailureParsingException("error", "node1", "index", 1, "cluster2"),
            createShardFailureParsingException("error", "node2", "index", 2, "cluster2"),
            createShardFailureParsingException("another error", "node2", "index", 2, "cluster2") };

        ShardOperationFailedException[] groupBy = ExceptionsHelper.groupBy(failures);
        assertThat(groupBy.length, equalTo(5));
        String[] expectedIndices = new String[] { "index", "index2", "cluster1:index", "cluster2:index", "cluster2:index" };
        String[] expectedErrors = new String[] { "error", "error", "error", "error", "another error" };
        int i = 0;
        for (ShardOperationFailedException shardOperationFailedException : groupBy) {
            assertThat(shardOperationFailedException.getCause().getMessage(), equalTo(expectedErrors[i]));
            assertThat(shardOperationFailedException.index(), equalTo(expectedIndices[i++]));
        }
    }

    private static ShardSearchFailure createShardFailureParsingException(
        String error,
        String nodeId,
        String index,
        int shardId,
        String clusterAlias
    ) {
        ParsingException ex = new ParsingException(0, 0, error, new IllegalArgumentException("some bad argument"));
        ex.setIndex(index);
        return new ShardSearchFailure(ex, createSearchShardTarget(nodeId, shardId, index, clusterAlias));
    }

    private static SearchShardTarget createSearchShardTarget(String nodeId, int shardId, String index, String clusterAlias) {
        return new SearchShardTarget(nodeId, new ShardId(new Index(index, IndexMetadata.INDEX_UUID_NA_VALUE), shardId), clusterAlias);
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
            createShardFailureQueryShardException("another error", "index2", null), };

        ShardOperationFailedException[] groupBy = ExceptionsHelper.groupBy(failures);
        assertThat(groupBy.length, equalTo(5));
        String[] expectedIndices = new String[] { "index", "cluster1:index", "cluster2:index", "index2", "index2" };
        String[] expectedErrors = new String[] { "error", "error", "error", "error", "another error" };
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
            new ShardSearchFailure(new ParsingException(0, 0, "error", null)), };

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

    public void testLimitedStackTrace() {
        // A normal exception is thrown several stack frames down
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        int expectedLength = 1 + maxTraces + 1; // Exception message, traces, then the count of remaining traces
        assertThat(limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceShortened() {
        // An exception has a smaller trace than is requested
        // Set max traces very, very high, since this test is sensitive to the number of method calls on the thread's stack.
        int maxTraces = 5000;
        RuntimeException exception = new RuntimeException("Regular Exception");
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        String fullTrace = ExceptionsHelper.stackTrace(exception);
        int expectedLength = fullTrace.split("\n").length; // The resulting line count should not be reduced
        assertThat(limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceWrappedExceptions() {
        // An exception is thrown and is then wrapped several stack frames later
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(
            randomIntBetween(10, 15),
            () -> throwExceptionCausedBy(recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException))
        );
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        // (1) Exception message, (n) traces, (1) remaining traces, (1) caused by, (n) caused by traces, (1) remaining traces
        int expectedLength = 4 + (2 * maxTraces);
        assertThat(limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceSuppressingAnException() {
        // A normal exception is thrown several stack frames down and then suppresses a new exception on the way back up
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(randomIntBetween(10, 15), () -> {
            RuntimeException original = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
            recurseUntil(randomIntBetween(10, 15), () -> suppressNewExceptionUnder(original));
        });
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        // (1) Exception message, (n) traces, (1) remaining traces, then
        // (1) suppressed, (n) suppressed by traces, (1) remaining lines
        int expectedLength = 4 + (2 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceSuppressedByAnException() {
        // A normal exception is thrown several stack frames down and then gets suppressed on the way back up by a new exception
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(randomIntBetween(10, 15), () -> {
            RuntimeException original = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
            recurseUntil(randomIntBetween(10, 15), () -> throwNewExceptionThatSuppresses(original));
        });
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        // (1) Exception message, (n) traces, (1) remaining traces, then
        // (1) suppressed original exception, (n) suppressed traces, (1) remaining traces
        int expectedLength = 4 + (2 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceSuppressingAnExceptionWithACause() {
        // A normal exception is thrown several stack frames down. On the way back up, a new exception with a nested cause is
        // suppressed by it.
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(randomIntBetween(10, 15), () -> {
            RuntimeException original = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
            RuntimeException causedBy = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
            recurseUntil(randomIntBetween(10, 15), () -> suppressNewExceptionWithCauseUnder(original, causedBy));
        });
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        // (1) Exception message, (n) traces, (1) remaining traces, then
        // (1) suppressed exception, (n) suppressed traces, (1) remaining traces
        // (1) suppressed caused by exception, (n) traces, (1) remaining traces
        int expectedLength = 6 + (3 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceSuppressedByAnExceptionWithACause() {
        // A normal exception is thrown several stack frames down. On the way back up, a new exception with a nested cause
        // suppresses it.
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(randomIntBetween(10, 15), () -> {
            RuntimeException original = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
            RuntimeException causedBy = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
            recurseUntil(randomIntBetween(10, 15), () -> throwNewExceptionWithCauseThatSuppresses(original, causedBy));
        });
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        // (1) Exception message, (n) traces, (1) remaining traces, then
        // (1) suppressed original exception, (n) traces, (1) remaining traces, then
        // (1) caused by exception, (n) traces, (1) remaining traces
        int expectedLength = 6 + (3 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceWrappedAndSuppressingAWrappedException() {
        // A normal exception is thrown several stack frames down. It gets wrapped on the way back up.
        // Some "recovery" code runs and a new exception is thrown. It also gets wrapped on the way back up.
        // The first chain of exceptions suppresses the second.
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(randomIntBetween(10, 15), () -> {
            RuntimeException original = recurseAndCatchRuntime(
                randomIntBetween(10, 15),
                () -> throwExceptionCausedBy(recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException))
            );
            RuntimeException causedBy = recurseAndCatchRuntime(
                randomIntBetween(10, 15),
                () -> throwExceptionCausedBy(recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException))
            );
            recurseUntil(randomIntBetween(10, 15), () -> suppressNewExceptionWithCauseUnder(original, causedBy));
        });
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        // (1) wrapped exception message, (n) traces, (1) remaining traces, then
        // (1) suppressed exception, (n) suppressed traces, (1) remaining traces, then
        // (1) wrapped exception under suppressed exception, (n) traces, (1) remaining traces, then
        // (1) root cause of suppressed exception chain, (n) traces, (1) remaining traces, then
        // (1) root cause of the suppressing exception chain, (n) traces, (1) remaining traces
        int expectedLength = 10 + (5 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceWrappedAndSuppressedByAWrappedException() {
        // A normal exception is thrown several stack frames down. It gets wrapped on the way back up.
        // Some "recovery" code runs and a new exception is thrown. It also gets wrapped on the way back up.
        // The first chain of exceptions suppresses the second.
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(randomIntBetween(10, 15), () -> {
            RuntimeException original = recurseAndCatchRuntime(
                randomIntBetween(10, 15),
                () -> throwExceptionCausedBy(recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException))
            );
            RuntimeException causedBy = recurseAndCatchRuntime(
                randomIntBetween(10, 15),
                () -> throwExceptionCausedBy(recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException))
            );
            throwNewExceptionWithCauseThatSuppresses(original, causedBy);
        });
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        // (1) wrapped exception message, (n) traces, (1) remaining traces, then
        // (1) suppressed wrapped exception, (n) traces, (1) remaining traces, then
        // (1) root cause of suppressed exception chain, (n) traces, (1) remaining traces, then
        // (1) wrapped exception, (n) traces, (1) remaining traces, then
        // (1) root cause of exception chain, (n) traces, (1) remaining traces
        int expectedLength = 10 + (5 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceNestedSuppression() {
        // The original exception is thrown and is then repeatedly suppressed by new exceptions
        int maxTraces = between(0, 5);
        RuntimeException exception = recurseAndCatchRuntime(
            randomIntBetween(10, 15),
            () -> throwNewExceptionWithCauseThatSuppresses(
                recurseAndCatchRuntime(
                    randomIntBetween(10, 15),
                    () -> throwNewExceptionWithCauseThatSuppresses(
                        recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException),
                        recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException)
                    )
                ),
                recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException)
            )
        );
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception, maxTraces);
        // (1) first suppressing exception message, (n) traces, (1) remaining traces, then
        // (1) second suppressing exception message, (n) traces, (1) remaining traces, then
        // (1) suppressed original exception message, (n) traces, (1) remaining traces, then
        // (1) cause of second suppressed exception, (n) traces, (1) remaining traces, then
        // (1) cause of first suppressed exception, (n) traces, (1) remaining traces, then
        int expectedLength = 10 + (5 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceCircularCause() {
        // An exception is thrown and then suppresses itself
        int maxTraces = between(0, 5);
        RuntimeException exception1 = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
        RuntimeException exception2 = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
        exception1.initCause(exception2);
        exception2.initCause(exception1);
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception1, maxTraces);
        // (1) first exception message, (n) traces, (1) remaining traces, then
        // (1) caused by second exception, (n) traces, (1) remaining traces, then
        // (1) caused by first exception message again, but no further traces
        int expectedLength = 5 + (2 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    public void testLimitedStackTraceCircularSuppression() {
        // An exception is thrown and then suppresses itself
        int maxTraces = between(0, 5);
        RuntimeException exception1 = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
        RuntimeException exception2 = recurseAndCatchRuntime(randomIntBetween(10, 15), ExceptionsHelperTests::throwRegularException);
        exception1.addSuppressed(exception2);
        exception2.addSuppressed(exception1);
        String limitedTrace = ExceptionsHelper.limitedStackTrace(exception1, maxTraces);
        // (1) first exception message, (n) traces, (1) remaining traces, then
        // (1) suppressed second exception, (n) traces, (1) remaining traces, then
        // (1) suppressed first exception message again, but no further traces
        int expectedLength = 5 + (2 * maxTraces);
        assertThat(limitedTrace, limitedTrace.split("\n").length, equalTo(expectedLength));
    }

    private static void throwRegularException() {
        throw new RuntimeException("Regular Exception");
    }

    private static void throwExceptionCausedBy(RuntimeException causedBy) {
        throw new RuntimeException("Wrapping Exception", causedBy);
    }

    private static void suppressNewExceptionUnder(RuntimeException suppressor) {
        suppressor.addSuppressed(new RuntimeException("Suppressed Exception"));
        throw suppressor;
    }

    private static void throwNewExceptionThatSuppresses(RuntimeException suppressed) {
        RuntimeException priority = new RuntimeException("Priority Exception");
        priority.addSuppressed(suppressed);
        throw priority;
    }

    private static void suppressNewExceptionWithCauseUnder(RuntimeException suppressor, RuntimeException suppressedCause) {
        suppressor.addSuppressed(new RuntimeException("Suppressed Exception", suppressedCause));
        throw suppressor;
    }

    private static void throwNewExceptionWithCauseThatSuppresses(RuntimeException suppressed, RuntimeException suppressorCause) {
        RuntimeException priority = new RuntimeException("Priority Exception", suppressorCause);
        priority.addSuppressed(suppressed);
        throw priority;
    };

    private static RuntimeException recurseAndCatchRuntime(int depth, Runnable op) {
        return expectThrows(RuntimeException.class, () -> doRecurse(depth, 0, op));
    }

    private static void recurseUntil(int depth, Runnable op) {
        doRecurse(depth, 0, op);
    }

    private static void doRecurse(int depth, int current, Runnable op) {
        if (depth == current) {
            op.run();
        } else {
            doRecurse(depth, current + 1, op);
        }
    }

    public void testCompressStackTraceElement() {
        assertThat(compressPackages(""), equalTo(""));
        assertThat(compressPackages("."), equalTo(""));
        assertThat(compressPackages("ClassName"), equalTo("ClassName"));
        assertThat(compressPackages("alfa.ClassName"), equalTo("a.ClassName"));
        assertThat(compressPackages("alfa.bravo.ClassName"), equalTo("a.b.ClassName"));
        assertThat(compressPackages(".ClassName"), equalTo("ClassName"));
        assertThat(compressPackages(".alfa.ClassName"), equalTo("a.ClassName"));
        assertThat(compressPackages(".alfa.bravo.ClassName"), equalTo("a.b.ClassName"));
        assertThat(compressPackages("...alfa.....ClassName"), equalTo("a.ClassName"));
        assertThat(compressPackages("...alfa....bravo.ClassName"), equalTo("a.b.ClassName"));
        assertThat(compressPackages("...alfa....bravo.charliepackagenameisreallyreallylong.ClassName"), equalTo("a.b.c.ClassName"));
        assertThat(compressPackages("alfa.bravo.charlie.OuterClassName.InnerClassName"), equalTo("a.b.c.O.InnerClassName"));

        expectThrows(AssertionError.class, () -> compressPackages(null));
    }

    private static String compressPackages(String className) {
        StringBuilder s = new StringBuilder();
        ExceptionsHelper.compressPackages(s, className);
        return s.toString();
    }
}
