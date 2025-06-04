/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.exception;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.*;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.action.bulk.IndexDocFailureStoreStatus;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.client.internal.AbstractClientHeadersTestCase;
import org.elasticsearch.cluster.RemoteException;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.coordination.CoordinationStateRejectedException;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.coordination.NodeHealthCheckFailureException;
import org.elasticsearch.cluster.desirednodes.VersionConflictException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.IllegalShardRoutingStateException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreadsTests;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.health.node.action.HealthNodeNotDiscoveredException;
import org.elasticsearch.http.HttpHeadersValidationException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.elasticsearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException;
import org.elasticsearch.index.seqno.RetentionLeaseNotFoundException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotInPrimaryModeException;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.indices.FailureIndexNotSupportedException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.indices.recovery.PeerRecoveryNotFound;
import org.elasticsearch.indices.recovery.RecoverFilesRecoveryException;
import org.elasticsearch.indices.recovery.RecoveryCommitTooNewException;
import org.elasticsearch.ingest.GraphStructureException;
import org.elasticsearch.ingest.IngestPipelineException;
import org.elasticsearch.ingest.IngestProcessorException;
import org.elasticsearch.persistent.NotPersistentTaskNodeException;
import org.elasticsearch.persistent.PersistentTaskNodeNotAssignedException;
import org.elasticsearch.repositories.RepositoryConflictException;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.rest.ApiNotAvailableException;
import org.elasticsearch.rest.RestResponseTests;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.TooManyScrollContextsException;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.UnsupportedAggregationOnDownsampledIndex;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.query.SearchTimeoutException;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotNameAlreadyInUseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.ActionTransportException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.TcpTransport;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.AccessDeniedException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystemLoopException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isInterface;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class ExceptionSerializationTests extends ESTestCase {

    public void testExceptionRegistration() throws IOException, URISyntaxException {
        final Set<Class<?>> notRegistered = new HashSet<>();
        final Set<Class<?>> registered = new HashSet<>();
        final String path = "/org/elasticsearch";
        final Path startPath = PathUtils.get(ElasticsearchException.class.getProtectionDomain().getCodeSource().getLocation().toURI())
            .resolve("org")
            .resolve("elasticsearch");
        final Set<? extends Class<?>> ignore = Sets.newHashSet(
            CancellableThreadsTests.CustomException.class,
            RestResponseTests.WithHeadersException.class,
            AbstractClientHeadersTestCase.InternalException.class,
            ElasticsearchExceptionTests.ExceptionSubclass.class
        );
        FileVisitor<Path> visitor = new FileVisitor<Path>() {
            private Path pkgPrefix = PathUtils.get(path).getParent();

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                pkgPrefix = pkgPrefix.resolve(dir.getFileName());
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                checkFile(file.getFileName().toString());
                return FileVisitResult.CONTINUE;
            }

            private void checkFile(String filename) {
                if (filename.endsWith(".class") == false) {
                    return;
                }
                try {
                    checkClass(loadClass(filename));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }

            private void checkClass(Class<?> clazz) {
                if (ignore.contains(clazz) || isAbstract(clazz.getModifiers()) || isInterface(clazz.getModifiers())) {
                    return;
                }
                if (isEsException(clazz) == false) {
                    return;
                }
                if (ElasticsearchException.isRegistered(clazz.asSubclass(Throwable.class), TransportVersion.current()) == false
                    && ElasticsearchException.class.equals(clazz.getEnclosingClass()) == false) {
                    notRegistered.add(clazz);
                } else if (ElasticsearchException.isRegistered(clazz.asSubclass(Throwable.class), TransportVersion.current())) {
                    registered.add(clazz);
                }
            }

            private boolean isEsException(Class<?> clazz) {
                return ElasticsearchException.class.isAssignableFrom(clazz);
            }

            private Class<?> loadClass(String filename) throws ClassNotFoundException {
                StringBuilder pkg = new StringBuilder();
                for (Path p : pkgPrefix) {
                    pkg.append(p.getFileName().toString()).append(".");
                }
                pkg.append(filename, 0, filename.length() - 6);
                return getClass().getClassLoader().loadClass(pkg.toString());
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw exc;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                pkgPrefix = pkgPrefix.getParent();
                return FileVisitResult.CONTINUE;
            }
        };

        Files.walkFileTree(startPath, visitor);
        final Path testStartPath = PathUtils.get(ExceptionSerializationTests.class.getResource(path).toURI());
        Files.walkFileTree(testStartPath, visitor);
        assertTrue(notRegistered.remove(TestException.class));
        assertTrue(notRegistered.remove(UnknownHeaderException.class));
        assertTrue("Classes subclassing ElasticsearchException must be registered \n" + notRegistered, notRegistered.isEmpty());
        assertTrue(registered.removeAll(ElasticsearchException.getRegisteredKeys())); // check
        assertEquals(registered.toString(), 0, registered.size());
    }

    public static final class TestException extends ElasticsearchException {
        public TestException(StreamInput in) throws IOException {
            super(in);
        }
    }

    private <T extends Exception> T serialize(T exception) throws IOException {
        return serialize(exception, TransportVersionUtils.randomVersion(random()));
    }

    private <T extends Exception> T serialize(T exception, TransportVersion version) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.setTransportVersion(version);
        out.writeException(exception);

        StreamInput in = out.bytes().streamInput();
        in.setTransportVersion(version);
        return in.readException();
    }

    public void testIllegalShardRoutingStateException() throws IOException {
        final ShardRouting routing = newShardRouting("test", 0, "xyz", false, ShardRoutingState.STARTED);
        final String routingAsString = routing.toString();
        IllegalShardRoutingStateException serialize = serialize(
            new IllegalShardRoutingStateException(routing, "foo", new NullPointerException())
        );
        assertNotNull(serialize.shard());
        assertEquals(routing, serialize.shard());
        assertEquals(routingAsString + ": foo", serialize.getMessage());
        assertTrue(serialize.getCause() instanceof NullPointerException);

        serialize = serialize(new IllegalShardRoutingStateException(routing, "bar", null));
        assertNotNull(serialize.shard());
        assertEquals(routing, serialize.shard());
        assertEquals(routingAsString + ": bar", serialize.getMessage());
        assertNull(serialize.getCause());
    }

    public void testParsingException() throws IOException {
        ParsingException ex = serialize(new ParsingException(1, 2, "fobar", null));
        assertNull(ex.getIndex());
        assertEquals(ex.getMessage(), "fobar");
        assertEquals(ex.getLineNumber(), 1);
        assertEquals(ex.getColumnNumber(), 2);

        ex = serialize(new ParsingException(1, 2, null, null));
        assertNull(ex.getIndex());
        assertNull(ex.getMessage());
        assertEquals(ex.getLineNumber(), 1);
        assertEquals(ex.getColumnNumber(), 2);
    }

    public void testQueryShardException() throws IOException {
        QueryShardException ex = serialize(new QueryShardException(new Index("foo", "_na_"), "fobar", null));
        assertEquals(ex.getIndex().getName(), "foo");
        assertEquals(ex.getMessage(), "fobar");

        ex = serialize(new QueryShardException((Index) null, null, null));
        assertNull(ex.getIndex());
        assertNull(ex.getMessage());
    }

    public void testSearchException() throws IOException {
        SearchShardTarget target = new SearchShardTarget("foo", new ShardId("bar", "_na_", 1), null);
        SearchException ex = serialize(new SearchException(null, "hello world", new NullPointerException()));
        assertNull(ex.shard());
        assertEquals(ex.getMessage(), "hello world");
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testActionNotFoundTransportException() throws IOException {
        ActionNotFoundTransportException ex = serialize(new ActionNotFoundTransportException("AACCCTION"));
        assertEquals("AACCCTION", ex.action());
        assertEquals("No handler for action [AACCCTION]", ex.getMessage());
    }

    public void testSnapshotException() throws IOException {
        final Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap", UUIDs.randomBase64UUID()));
        SnapshotException ex = serialize(new SnapshotException(snapshot, "no such snapshot", new NullPointerException()));
        assertEquals(ex.getRepositoryName(), snapshot.getRepository());
        assertEquals(ex.getSnapshotName(), snapshot.getSnapshotId().getName());
        assertEquals(ex.getMessage(), "[" + snapshot + "] no such snapshot");
        assertTrue(ex.getCause() instanceof NullPointerException);

        ex = serialize(new SnapshotException(null, "no such snapshot", new NullPointerException()));
        assertNull(ex.getRepositoryName());
        assertNull(ex.getSnapshotName());
        assertEquals(ex.getMessage(), "[_na] no such snapshot");
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testRecoverFilesRecoveryException() throws IOException {
        ShardId id = new ShardId("foo", "_na_", 1);
        ByteSizeValue bytes = ByteSizeValue.ofBytes(randomIntBetween(0, 10000));
        RecoverFilesRecoveryException ex = serialize(new RecoverFilesRecoveryException(id, 10, bytes, null));
        assertEquals(ex.getShardId(), id);
        assertEquals(ex.numberOfFiles(), 10);
        assertEquals(ex.totalFilesSize(), bytes);
        assertEquals(ex.getMessage(), "Failed to transfer [10] files with total size of [" + bytes + "]");
        assertNull(ex.getCause());

        ex = serialize(new RecoverFilesRecoveryException(null, 10, bytes, new NullPointerException()));
        assertNull(ex.getShardId());
        assertEquals(ex.numberOfFiles(), 10);
        assertEquals(ex.totalFilesSize(), bytes);
        assertEquals(ex.getMessage(), "Failed to transfer [10] files with total size of [" + bytes + "]");
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testInvalidIndexTemplateException() throws IOException {
        InvalidIndexTemplateException ex = serialize(new InvalidIndexTemplateException("foo", "bar"));
        assertEquals(ex.getMessage(), "index_template [foo] invalid, cause [bar]");
        assertEquals(ex.name(), "foo");
        ex = serialize(new InvalidIndexTemplateException(null, "bar"));
        assertEquals(ex.getMessage(), "index_template [null] invalid, cause [bar]");
        assertNull(ex.name());
    }

    public void testActionTransportException() throws IOException {
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        ActionTransportException ex = serialize(new ActionTransportException("name?", transportAddress, "ACTION BABY!", "message?", null));
        assertEquals("[name?][" + transportAddress + "][ACTION BABY!] message?", ex.getMessage());
    }

    public void testSearchContextMissingException() throws IOException {
        ShardSearchContextId contextId = new ShardSearchContextId(UUIDs.randomBase64UUID(), randomLong());
        TransportVersion version = TransportVersionUtils.randomCompatibleVersion(random());
        SearchContextMissingException ex = serialize(new SearchContextMissingException(contextId), version);
        assertThat(ex.contextId().getId(), equalTo(contextId.getId()));
        assertThat(ex.contextId().getSessionId(), equalTo(contextId.getSessionId()));
    }

    public void testCircuitBreakingException() throws IOException {
        CircuitBreakingException ex = serialize(
            new CircuitBreakingException("Too large", 0, 100, CircuitBreaker.Durability.TRANSIENT),
            TransportVersions.V_8_0_0
        );
        assertEquals("Too large", ex.getMessage());
        assertEquals(100, ex.getByteLimit());
        assertEquals(0, ex.getBytesWanted());
        assertEquals(CircuitBreaker.Durability.TRANSIENT, ex.getDurability());
    }

    public void testTooManyBucketsException() throws IOException {
        TransportVersion version = TransportVersionUtils.randomCompatibleVersion(random());
        MultiBucketConsumerService.TooManyBucketsException ex = serialize(
            new MultiBucketConsumerService.TooManyBucketsException("Too many buckets", 100),
            version
        );
        assertEquals("Too many buckets", ex.getMessage());
        assertEquals(100, ex.getMaxBuckets());
    }

    public void testTimestampParsingException() throws IOException {
        TimestampParsingException ex = serialize(new TimestampParsingException("TIMESTAMP", null));
        assertEquals("failed to parse timestamp [TIMESTAMP]", ex.getMessage());
        assertEquals("TIMESTAMP", ex.timestamp());
    }

    public void testAliasesMissingException() throws IOException {
        AliasesNotFoundException ex = serialize(new AliasesNotFoundException("one", "two", "three"));
        assertEquals("aliases [one, two, three] missing", ex.getMessage());
        assertEquals("aliases", ex.getResourceType());
        assertArrayEquals(new String[] { "one", "two", "three" }, ex.getResourceId().toArray(new String[0]));
    }

    public void testIllegalIndexShardStateException() throws IOException {
        ShardId id = new ShardId("foo", "_na_", 1);
        IndexShardState state = randomFrom(IndexShardState.values());
        IllegalIndexShardStateException ex = serialize(new IllegalIndexShardStateException(id, state, "come back later buddy"));
        assertEquals(id, ex.getShardId());
        assertEquals("CurrentState[" + state.name() + "] come back later buddy", ex.getMessage());
        assertEquals(state, ex.currentState());
    }

    public void testConnectTransportException() throws IOException {
        TransportAddress transportAddress = buildNewFakeTransportAddress();
        DiscoveryNode node = DiscoveryNodeUtils.create("thenode", transportAddress, emptyMap(), emptySet());
        ConnectTransportException ex = serialize(new ConnectTransportException(node, "msg", "action", null));
        assertEquals("[][" + transportAddress + "][action] msg", ex.getMessage());
        assertNull(ex.getCause());

        ex = serialize(new ConnectTransportException(node, "msg", "action", new NullPointerException()));
        assertEquals("[][" + transportAddress + "][action] msg", ex.getMessage());
        assertThat(ex.getCause(), instanceOf(NullPointerException.class));
        assertEquals(RestStatus.BAD_GATEWAY, ex.status());
    }

    public void testSearchPhaseExecutionException() throws IOException {
        ShardSearchFailure[] empty = ShardSearchFailure.EMPTY_ARRAY;
        SearchPhaseExecutionException ex = serialize(new SearchPhaseExecutionException("boom", "baam", new NullPointerException(), empty));
        assertEquals("boom", ex.getPhaseName());
        assertEquals("baam", ex.getMessage());
        assertTrue(ex.getCause() instanceof NullPointerException);
        assertEquals(empty.length, ex.shardFailures().length);
        ShardSearchFailure[] one = new ShardSearchFailure[] { new ShardSearchFailure(new IllegalArgumentException("nono!")) };

        ex = serialize(new SearchPhaseExecutionException("boom", "baam", new NullPointerException(), one));
        assertEquals("boom", ex.getPhaseName());
        assertEquals("baam", ex.getMessage());
        assertTrue(ex.getCause() instanceof NullPointerException);
        assertEquals(one.length, ex.shardFailures().length);
        assertTrue(ex.shardFailures()[0].getCause() instanceof IllegalArgumentException);
    }

    public void testRoutingMissingException() throws IOException {
        RoutingMissingException ex = serialize(new RoutingMissingException("idx", "id"));
        assertEquals("idx", ex.getIndex().getName());
        assertEquals("id", ex.getId());
        assertEquals("routing is required for [idx]/[id]", ex.getMessage());
    }

    public void testRepositoryException() throws IOException {
        RepositoryException ex = serialize(new RepositoryException("repo", "msg"));
        assertEquals("repo", ex.repository());
        assertEquals("[repo] msg", ex.getMessage());

        ex = serialize(new RepositoryException(null, "msg"));
        assertNull(ex.repository());
        assertEquals("[_na] msg", ex.getMessage());
    }

    public void testIndexTemplateMissingException() throws IOException {
        IndexTemplateMissingException ex = serialize(new IndexTemplateMissingException("name"));
        assertEquals("index_template [name] missing", ex.getMessage());
        assertEquals("name", ex.name());

        ex = serialize(new IndexTemplateMissingException((String) null));
        assertEquals("index_template [null] missing", ex.getMessage());
        assertNull(ex.name());
    }

    public void testRecoveryEngineException() throws IOException {
        ShardId id = new ShardId("foo", "_na_", 1);
        RecoveryEngineException ex = serialize(new RecoveryEngineException(id, 10, "total failure", new NullPointerException()));
        assertEquals(id, ex.getShardId());
        assertEquals("Phase[10] total failure", ex.getMessage());
        assertEquals(10, ex.phase());
        ex = serialize(new RecoveryEngineException(null, -1, "total failure", new NullPointerException()));
        assertNull(ex.getShardId());
        assertEquals(-1, ex.phase());
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testFailedNodeException() throws IOException {
        FailedNodeException ex = serialize(new FailedNodeException("the node", "the message", null));
        assertEquals("the node", ex.nodeId());
        assertEquals("the message", ex.getMessage());
    }

    public void testClusterBlockException() throws IOException {
        ClusterBlockException ex = serialize(new ClusterBlockException(singleton(NoMasterBlockService.NO_MASTER_BLOCK_WRITES)));
        assertEquals("blocked by: [SERVICE_UNAVAILABLE/2/no master];", ex.getMessage());
        assertTrue(ex.blocks().contains(NoMasterBlockService.NO_MASTER_BLOCK_WRITES));
        assertEquals(1, ex.blocks().size());
    }

    public void testNotSerializableExceptionWrapper() throws IOException {
        NotSerializableExceptionWrapper ex = serialize(new NotSerializableExceptionWrapper(new NullPointerException()));
        assertEquals("""
            {"type":"null_pointer_exception","reason":"null_pointer_exception: null"}""", Strings.toString(ex));
        ex = serialize(new NotSerializableExceptionWrapper(new IllegalArgumentException("nono!")));
        assertEquals("""
            {"type":"illegal_argument_exception","reason":"illegal_argument_exception: nono!"}""", Strings.toString(ex));

        class UnknownException extends Exception {
            UnknownException(final String message) {
                super(message);
            }
        }

        Exception[] unknowns = new Exception[] {
            new Exception("foobar"),
            new ClassCastException("boom boom boom"),
            new UnknownException("boom") };
        for (Exception t : unknowns) {
            if (randomBoolean()) {
                t.addSuppressed(new UnsatisfiedLinkError("suppressed"));
                t.addSuppressed(new NullPointerException());
            }
            Throwable deserialized = serialize(t);
            assertTrue(deserialized.getClass().toString(), deserialized instanceof NotSerializableExceptionWrapper);
            assertArrayEquals(t.getStackTrace(), deserialized.getStackTrace());
            assertEquals(t.getSuppressed().length, deserialized.getSuppressed().length);
            if (t.getSuppressed().length > 0) {
                assertTrue(deserialized.getSuppressed()[0] instanceof NotSerializableExceptionWrapper);
                assertArrayEquals(t.getSuppressed()[0].getStackTrace(), deserialized.getSuppressed()[0].getStackTrace());
                assertTrue(deserialized.getSuppressed()[1] instanceof NullPointerException);
            }
        }
    }

    public void testUnknownException() throws IOException {
        ParsingException parsingException = new ParsingException(1, 2, "foobar", null);
        final Exception ex = new UnknownException("eggplant", parsingException);
        Exception exception = serialize(ex);
        assertEquals("unknown_exception: eggplant", exception.getMessage());
        assertTrue(exception instanceof ElasticsearchException);
        ParsingException e = (ParsingException) exception.getCause();
        assertEquals(parsingException.getIndex(), e.getIndex());
        assertEquals(parsingException.getMessage(), e.getMessage());
        assertEquals(parsingException.getLineNumber(), e.getLineNumber());
        assertEquals(parsingException.getColumnNumber(), e.getColumnNumber());
    }

    public void testWriteThrowable() throws IOException {
        final QueryShardException queryShardException = new QueryShardException(new Index("foo", "_na_"), "foobar", null);
        final UnknownException unknownException = new UnknownException("this exception is unknown", queryShardException);

        final Exception[] causes = new Exception[] {
            new IllegalStateException("foobar"),
            new IllegalArgumentException("alalaal"),
            new NullPointerException("boom"),
            new EOFException("dadada"),
            new ElasticsearchSecurityException("nono!"),
            new NumberFormatException("not a number"),
            new CorruptIndexException("baaaam booom", "this is my resource"),
            new IndexFormatTooNewException("tooo new", 1, 2, 3),
            new IndexFormatTooOldException("tooo new", 1, 2, 3),
            new IndexFormatTooOldException("tooo new", "very old version"),
            new ArrayIndexOutOfBoundsException("booom"),
            new StringIndexOutOfBoundsException("booom"),
            new FileNotFoundException("booom"),
            new NoSuchFileException("booom"),
            new AlreadyClosedException("closed!!", new NullPointerException()),
            new LockObtainFailedException("can't lock directory", new NullPointerException()),
            unknownException };
        for (final Exception cause : causes) {
            ElasticsearchException ex = new ElasticsearchException("topLevel", cause);
            ElasticsearchException deserialized = serialize(ex);
            assertEquals(deserialized.getMessage(), ex.getMessage());
            assertTrue(
                "Expected: "
                    + deserialized.getCause().getMessage()
                    + " to contain: "
                    + ex.getCause().getClass().getName()
                    + " but it didn't",
                deserialized.getCause().getMessage().contains(ex.getCause().getMessage())
            );
            if (ex.getCause().getClass() != UnknownException.class) { // unknown exception is not directly mapped
                assertEquals(deserialized.getCause().getClass(), ex.getCause().getClass());
            } else {
                assertEquals(deserialized.getCause().getClass(), NotSerializableExceptionWrapper.class);
            }
            assertArrayEquals(deserialized.getStackTrace(), ex.getStackTrace());
            assertTrue(deserialized.getStackTrace().length > 1);
        }
    }

    public void testWithRestHeadersException() throws IOException {
        {
            ElasticsearchException ex = new ElasticsearchException("msg");
            ex.addHeader("foo", "foo", "bar");
            ex.addMetadata("es.foo_metadata", "value1", "value2");
            ex = serialize(ex);
            assertEquals("msg", ex.getMessage());
            assertEquals(2, ex.getHeader("foo").size());
            assertEquals("foo", ex.getHeader("foo").get(0));
            assertEquals("bar", ex.getHeader("foo").get(1));
            assertEquals(2, ex.getMetadata("es.foo_metadata").size());
            assertEquals("value1", ex.getMetadata("es.foo_metadata").get(0));
            assertEquals("value2", ex.getMetadata("es.foo_metadata").get(1));
        }
        {
            RestStatus status = randomFrom(RestStatus.values());
            // ensure we are carrying over the headers and metadata even if not serialized
            UnknownHeaderException uhe = new UnknownHeaderException("msg", status);
            uhe.addHeader("foo", "foo", "bar");
            uhe.addMetadata("es.foo_metadata", "value1", "value2");

            ElasticsearchException serialize = serialize((ElasticsearchException) uhe);
            assertTrue(serialize instanceof NotSerializableExceptionWrapper);
            NotSerializableExceptionWrapper e = (NotSerializableExceptionWrapper) serialize;
            assertEquals("unknown_header_exception: msg", e.getMessage());
            assertEquals(2, e.getHeader("foo").size());
            assertEquals("foo", e.getHeader("foo").get(0));
            assertEquals("bar", e.getHeader("foo").get(1));
            assertEquals(2, e.getMetadata("es.foo_metadata").size());
            assertEquals("value1", e.getMetadata("es.foo_metadata").get(0));
            assertEquals("value2", e.getMetadata("es.foo_metadata").get(1));
            assertSame(status, e.status());
        }
    }

    public void testNoLongerPrimaryShardException() throws IOException {
        ShardId shardId = new ShardId(new Index(randomAlphaOfLength(4), randomAlphaOfLength(4)), randomIntBetween(0, Integer.MAX_VALUE));
        String msg = randomAlphaOfLength(4);
        ShardStateAction.NoLongerPrimaryShardException ex = serialize(new ShardStateAction.NoLongerPrimaryShardException(shardId, msg));
        assertEquals(shardId, ex.getShardId());
        assertEquals(msg, ex.getMessage());
    }

    public static class UnknownHeaderException extends ElasticsearchException {
        private final RestStatus status;

        UnknownHeaderException(String msg, RestStatus status) {
            super(msg);
            this.status = status;
        }

        @Override
        public RestStatus status() {
            return status;
        }
    }

    public void testElasticsearchSecurityException() throws IOException {
        ElasticsearchSecurityException ex = new ElasticsearchSecurityException("user [{}] is not allowed", RestStatus.UNAUTHORIZED, "foo");
        ElasticsearchSecurityException e = serialize(ex);
        assertEquals(ex.status(), e.status());
        assertEquals(RestStatus.UNAUTHORIZED, e.status());
    }

    public void testInterruptedException() throws IOException {
        InterruptedException orig = randomBoolean() ? new InterruptedException("boom") : new InterruptedException();
        InterruptedException ex = serialize(orig);
        assertEquals(orig.getMessage(), ex.getMessage());
    }

    public void testThatIdsArePositive() {
        for (final int id : ElasticsearchException.ids()) {
            assertThat("negative id", id, greaterThanOrEqualTo(0));
        }
    }

    public void testThatIdsAreUnique() {
        final Set<Integer> ids = new HashSet<>();
        for (final int id : ElasticsearchException.ids()) {
            assertTrue("duplicate id", ids.add(id));
        }
    }

    public void testIds() {
        Map<Integer, Class<? extends ElasticsearchException>> ids = new HashMap<>();
        ids.put(0, org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException.class);
        ids.put(1, org.elasticsearch.search.dfs.DfsPhaseExecutionException.class);
        ids.put(2, org.elasticsearch.common.util.CancellableThreads.ExecutionCancelledException.class);
        ids.put(3, org.elasticsearch.discovery.MasterNotDiscoveredException.class);
        ids.put(4, ElasticsearchSecurityException.class);
        ids.put(5, org.elasticsearch.index.snapshots.IndexShardRestoreException.class);
        ids.put(6, org.elasticsearch.indices.IndexClosedException.class);
        ids.put(7, org.elasticsearch.http.BindHttpException.class);
        ids.put(8, org.elasticsearch.action.search.ReduceSearchPhaseException.class);
        ids.put(9, org.elasticsearch.node.NodeClosedException.class);
        ids.put(10, null); // SnapshotFailedEngineException, never instantiated in 6.2.0+ and never thrown across clusters
        ids.put(11, org.elasticsearch.index.shard.ShardNotFoundException.class);
        ids.put(12, org.elasticsearch.transport.ConnectTransportException.class);
        ids.put(13, org.elasticsearch.transport.NotSerializableTransportException.class);
        ids.put(14, org.elasticsearch.transport.ResponseHandlerFailureTransportException.class);
        ids.put(15, org.elasticsearch.indices.IndexCreationException.class);
        ids.put(16, org.elasticsearch.index.IndexNotFoundException.class);
        ids.put(17, org.elasticsearch.cluster.routing.IllegalShardRoutingStateException.class);
        ids.put(18, org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException.class);
        ids.put(19, ResourceNotFoundException.class);
        ids.put(20, org.elasticsearch.transport.ActionTransportException.class);
        ids.put(21, ElasticsearchGenerationException.class);
        ids.put(22, null); // was CreateFailedEngineException
        ids.put(23, org.elasticsearch.index.shard.IndexShardStartedException.class);
        ids.put(24, org.elasticsearch.search.SearchContextMissingException.class);
        ids.put(25, org.elasticsearch.script.GeneralScriptException.class);
        ids.put(26, null);
        ids.put(27, null); // was SnapshotCreationException
        ids.put(28, null); // was DeleteFailedEngineException, deprecated in 6.0 and removed in 7.0
        ids.put(29, org.elasticsearch.index.engine.DocumentMissingException.class);
        ids.put(30, org.elasticsearch.snapshots.SnapshotException.class);
        ids.put(31, org.elasticsearch.indices.InvalidAliasNameException.class);
        ids.put(32, org.elasticsearch.indices.InvalidIndexNameException.class);
        ids.put(33, org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException.class);
        ids.put(34, org.elasticsearch.transport.TransportException.class);
        ids.put(35, ElasticsearchParseException.class);
        ids.put(36, org.elasticsearch.search.SearchException.class);
        ids.put(37, org.elasticsearch.index.mapper.MapperException.class);
        ids.put(38, org.elasticsearch.indices.InvalidTypeNameException.class);
        ids.put(39, org.elasticsearch.snapshots.SnapshotRestoreException.class);
        ids.put(40, org.elasticsearch.common.ParsingException.class);
        ids.put(41, org.elasticsearch.index.shard.IndexShardClosedException.class);
        ids.put(42, org.elasticsearch.indices.recovery.RecoverFilesRecoveryException.class);
        ids.put(43, org.elasticsearch.index.translog.TruncatedTranslogException.class);
        ids.put(44, org.elasticsearch.indices.recovery.RecoveryFailedException.class);
        ids.put(45, org.elasticsearch.index.shard.IndexShardRelocatedException.class);
        ids.put(46, null); // NodeShouldNotConnectException, never instantiated in 5.0+
        ids.put(47, null);
        ids.put(48, org.elasticsearch.index.translog.TranslogCorruptedException.class);
        ids.put(49, org.elasticsearch.cluster.block.ClusterBlockException.class);
        ids.put(50, org.elasticsearch.search.fetch.FetchPhaseExecutionException.class);
        ids.put(51, null);
        ids.put(52, org.elasticsearch.index.engine.VersionConflictEngineException.class);
        ids.put(53, org.elasticsearch.index.engine.EngineException.class);
        ids.put(54, null); // was DocumentAlreadyExistsException, which is superseded with VersionConflictEngineException
        ids.put(55, org.elasticsearch.action.NoSuchNodeException.class);
        ids.put(56, org.elasticsearch.common.settings.SettingsException.class);
        ids.put(57, org.elasticsearch.indices.IndexTemplateMissingException.class);
        ids.put(58, org.elasticsearch.transport.SendRequestTransportException.class);
        ids.put(59, null); // was EsRejectedExecutionException, which is no longer an instance of ElasticsearchException
        ids.put(60, null); // EarlyTerminationException was removed in 6.0
        ids.put(61, null); // RoutingValidationException was removed in 5.0
        ids.put(62, org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper.class);
        ids.put(63, org.elasticsearch.indices.AliasFilterParsingException.class);
        ids.put(64, null); // DeleteByQueryFailedEngineException was removed in 3.0
        ids.put(65, null); // GatewayException, never instantiated in 5.0+
        ids.put(66, org.elasticsearch.index.shard.IndexShardNotRecoveringException.class);
        ids.put(67, org.elasticsearch.http.HttpException.class);
        ids.put(68, ElasticsearchException.class);
        ids.put(69, org.elasticsearch.snapshots.SnapshotMissingException.class);
        ids.put(70, org.elasticsearch.action.PrimaryMissingActionException.class);
        ids.put(71, org.elasticsearch.action.FailedNodeException.class);
        ids.put(72, null); // was SearchParseException, only used in tests since 7.11
        ids.put(73, org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException.class);
        ids.put(74, org.elasticsearch.common.blobstore.BlobStoreException.class);
        ids.put(75, org.elasticsearch.cluster.IncompatibleClusterStateVersionException.class);
        ids.put(76, org.elasticsearch.index.engine.RecoveryEngineException.class);
        ids.put(77, org.elasticsearch.common.util.concurrent.UncategorizedExecutionException.class);
        ids.put(78, org.elasticsearch.action.TimestampParsingException.class);
        ids.put(79, org.elasticsearch.action.RoutingMissingException.class);
        ids.put(80, null); // was IndexFailedEngineException, deprecated in 6.0 and removed in 7.0
        ids.put(81, org.elasticsearch.index.snapshots.IndexShardRestoreFailedException.class);
        ids.put(82, org.elasticsearch.repositories.RepositoryException.class);
        ids.put(83, org.elasticsearch.transport.ReceiveTimeoutTransportException.class);
        ids.put(84, org.elasticsearch.transport.NodeDisconnectedException.class);
        ids.put(85, null);
        ids.put(86, org.elasticsearch.search.aggregations.AggregationExecutionException.class);
        ids.put(88, org.elasticsearch.indices.InvalidIndexTemplateException.class);
        ids.put(90, org.elasticsearch.index.engine.RefreshFailedEngineException.class);
        ids.put(91, org.elasticsearch.search.aggregations.AggregationInitializationException.class);
        ids.put(92, org.elasticsearch.indices.recovery.DelayRecoveryException.class);
        ids.put(94, org.elasticsearch.client.internal.transport.NoNodeAvailableException.class);
        ids.put(95, null);
        ids.put(96, org.elasticsearch.snapshots.InvalidSnapshotNameException.class);
        ids.put(97, org.elasticsearch.index.shard.IllegalIndexShardStateException.class);
        ids.put(98, org.elasticsearch.index.snapshots.IndexShardSnapshotException.class);
        ids.put(99, org.elasticsearch.index.shard.IndexShardNotStartedException.class);
        ids.put(100, org.elasticsearch.action.search.SearchPhaseExecutionException.class);
        ids.put(101, org.elasticsearch.transport.ActionNotFoundTransportException.class);
        ids.put(102, org.elasticsearch.transport.TransportSerializationException.class);
        ids.put(103, org.elasticsearch.transport.RemoteTransportException.class);
        ids.put(104, org.elasticsearch.index.engine.EngineCreationFailureException.class);
        ids.put(105, org.elasticsearch.cluster.routing.RoutingException.class);
        ids.put(106, org.elasticsearch.index.shard.IndexShardRecoveryException.class);
        ids.put(107, org.elasticsearch.repositories.RepositoryMissingException.class);
        ids.put(108, null);
        ids.put(109, org.elasticsearch.index.engine.DocumentSourceMissingException.class);
        ids.put(110, null); // FlushNotAllowedEngineException was removed in 5.0
        ids.put(111, org.elasticsearch.common.settings.NoClassSettingsException.class);
        ids.put(112, org.elasticsearch.transport.BindTransportException.class);
        ids.put(113, org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException.class);
        ids.put(114, org.elasticsearch.index.shard.IndexShardRecoveringException.class);
        ids.put(115, org.elasticsearch.index.translog.TranslogException.class);
        ids.put(116, org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException.class);
        ids.put(117, ReplicationOperation.RetryOnPrimaryException.class);
        ids.put(118, ElasticsearchTimeoutException.class);
        ids.put(119, org.elasticsearch.search.query.QueryPhaseExecutionException.class);
        ids.put(120, org.elasticsearch.repositories.RepositoryVerificationException.class);
        ids.put(121, org.elasticsearch.search.aggregations.InvalidAggregationPathException.class);
        ids.put(122, null);
        ids.put(123, ResourceAlreadyExistsException.class);
        ids.put(124, null);
        ids.put(125, TcpTransport.HttpRequestOnTransportException.class);
        ids.put(126, org.elasticsearch.index.mapper.MapperParsingException.class);
        ids.put(127, null); // was org.elasticsearch.search.SearchContextException.class
        ids.put(128, null); // was org.elasticsearch.search.builder.SearchSourceBuilderException.class
        ids.put(129, null); // was org.elasticsearch.index.engine.EngineClosedException.class
        ids.put(130, org.elasticsearch.action.NoShardAvailableActionException.class);
        ids.put(131, org.elasticsearch.action.UnavailableShardsException.class);
        ids.put(132, org.elasticsearch.index.engine.FlushFailedEngineException.class);
        ids.put(133, org.elasticsearch.common.breaker.CircuitBreakingException.class);
        ids.put(134, org.elasticsearch.transport.NodeNotConnectedException.class);
        ids.put(135, org.elasticsearch.index.mapper.StrictDynamicMappingException.class);
        ids.put(136, org.elasticsearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class);
        ids.put(137, org.elasticsearch.indices.TypeMissingException.class);
        ids.put(138, null);
        ids.put(139, null);
        ids.put(140, org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException.class);
        ids.put(141, org.elasticsearch.index.query.QueryShardException.class);
        ids.put(142, ShardStateAction.NoLongerPrimaryShardException.class);
        ids.put(143, org.elasticsearch.script.ScriptException.class);
        ids.put(144, org.elasticsearch.cluster.NotMasterException.class);
        ids.put(145, ElasticsearchStatusException.class);
        ids.put(146, org.elasticsearch.tasks.TaskCancelledException.class);
        ids.put(147, org.elasticsearch.env.ShardLockObtainFailedException.class);
        ids.put(148, null);
        ids.put(149, MultiBucketConsumerService.TooManyBucketsException.class);
        ids.put(150, CoordinationStateRejectedException.class);
        ids.put(151, SnapshotInProgressException.class);
        ids.put(152, NoSuchRemoteClusterException.class);
        ids.put(153, RetentionLeaseAlreadyExistsException.class);
        ids.put(154, RetentionLeaseNotFoundException.class);
        ids.put(155, ShardNotInPrimaryModeException.class);
        ids.put(156, RetentionLeaseInvalidRetainingSeqNoException.class);
        ids.put(157, IngestProcessorException.class);
        ids.put(158, PeerRecoveryNotFound.class);
        ids.put(159, NodeHealthCheckFailureException.class);
        ids.put(160, NoSeedNodeLeftException.class);
        ids.put(161, null);   // was org.elasticsearch.action.search.VersionMismatchException.class
        ids.put(162, ElasticsearchAuthenticationProcessingError.class);
        ids.put(163, RepositoryConflictException.class);
        ids.put(164, VersionConflictException.class);
        ids.put(165, SnapshotNameAlreadyInUseException.class);
        ids.put(166, HealthNodeNotDiscoveredException.class);
        ids.put(167, UnsupportedAggregationOnDownsampledIndex.class);
        ids.put(168, DocumentParsingException.class);
        ids.put(169, HttpHeadersValidationException.class);
        ids.put(170, ElasticsearchRoleRestrictionException.class);
        ids.put(171, ApiNotAvailableException.class);
        ids.put(172, RecoveryCommitTooNewException.class);
        ids.put(173, TooManyScrollContextsException.class);
        ids.put(174, AggregationExecutionException.InvalidPath.class);
        ids.put(175, AutoscalingMissedIndicesUpdateException.class);
        ids.put(176, SearchTimeoutException.class);
        ids.put(177, GraphStructureException.class);
        ids.put(178, FailureIndexNotSupportedException.class);
        ids.put(179, NotPersistentTaskNodeException.class);
        ids.put(180, PersistentTaskNodeNotAssignedException.class);
        ids.put(181, ResourceAlreadyUploadedException.class);
        ids.put(182, IngestPipelineException.class);
        ids.put(183, IndexDocFailureStoreStatus.ExceptionWithFailureStoreStatus.class);
        ids.put(184, RemoteException.class);

        Map<Class<? extends ElasticsearchException>, Integer> reverse = new HashMap<>();
        for (Map.Entry<Integer, Class<? extends ElasticsearchException>> entry : ids.entrySet()) {
            if (entry.getValue() != null) {
                reverse.put(entry.getValue(), entry.getKey());
            }
        }

        for (final Tuple<Integer, Class<? extends ElasticsearchException>> tuple : ElasticsearchException.classes()) {
            assertNotNull(tuple.v1());
            assertEquals((int) reverse.get(tuple.v2()), (int) tuple.v1());
        }

        for (Map.Entry<Integer, Class<? extends ElasticsearchException>> entry : ids.entrySet()) {
            if (entry.getValue() != null) {
                assertEquals((int) entry.getKey(), ElasticsearchException.getId(entry.getValue()));
            }
        }
    }

    public void testIOException() throws IOException {
        IOException serialize = serialize(new IOException("boom", new NullPointerException()));
        assertEquals("boom", serialize.getMessage());
        assertTrue(serialize.getCause() instanceof NullPointerException);
    }

    public void testFileSystemExceptions() throws IOException {
        for (FileSystemException ex : Arrays.asList(
            new FileSystemException("a", "b", "c"),
            new NoSuchFileException("a", "b", "c"),
            new NotDirectoryException("a"),
            new DirectoryNotEmptyException("a"),
            new AtomicMoveNotSupportedException("a", "b", "c"),
            new FileAlreadyExistsException("a", "b", "c"),
            new AccessDeniedException("a", "b", "c"),
            new FileSystemLoopException("a")
        )) {

            FileSystemException serialize = serialize(ex);
            assertEquals(serialize.getClass(), ex.getClass());
            assertEquals("a", serialize.getFile());
            if (serialize.getClass() == NotDirectoryException.class
                || serialize.getClass() == FileSystemLoopException.class
                || serialize.getClass() == DirectoryNotEmptyException.class) {
                assertNull(serialize.getOtherFile());
                assertNull(serialize.getReason());
            } else {
                assertEquals(serialize.getClass().toString(), "b", serialize.getOtherFile());
                assertEquals(serialize.getClass().toString(), "c", serialize.getReason());
            }
        }
    }

    public void testElasticsearchRemoteException() throws IOException {
        ElasticsearchStatusException ex = new ElasticsearchStatusException("something", RestStatus.TOO_MANY_REQUESTS);
        ElasticsearchStatusException e = serialize(ex);
        assertEquals(ex.status(), e.status());
        assertEquals(RestStatus.TOO_MANY_REQUESTS, e.status());
    }

    public void testShardLockObtainFailedException() throws IOException {
        ShardId shardId = new ShardId("foo", "_na_", 1);
        ShardLockObtainFailedException orig = new ShardLockObtainFailedException(shardId, "boom");
        TransportVersion version = TransportVersionUtils.randomCompatibleVersion(random());
        ShardLockObtainFailedException ex = serialize(orig, version);
        assertEquals(orig.getMessage(), ex.getMessage());
        assertEquals(orig.getShardId(), ex.getShardId());
    }

    public void testSnapshotInProgressException() throws IOException {
        SnapshotInProgressException orig = new SnapshotInProgressException("boom");
        TransportVersion version = TransportVersionUtils.randomCompatibleVersion(random());
        SnapshotInProgressException ex = serialize(orig, version);
        assertEquals(orig.getMessage(), ex.getMessage());
    }

    private static class UnknownException extends Exception {
        UnknownException(final String message, final Exception cause) {
            super(message, cause);
        }
    }
}
