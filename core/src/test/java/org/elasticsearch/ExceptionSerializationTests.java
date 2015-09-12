/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableSet;

import org.apache.lucene.util.Constants;
import org.codehaus.groovy.runtime.typehandling.GroovyCastException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.TimestampParsingException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.AbstractClientHeadersTestCase;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.SnapshotId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IllegalShardRoutingStateException;
import org.elasticsearch.cluster.routing.RoutingTableValidation;
import org.elasticsearch.cluster.routing.RoutingValidationException;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreadsTests;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.AlreadyExpiredException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.CreateFailedEngineException;
import org.elasticsearch.index.engine.IndexFailedEngineException;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.TranslogRecoveryPerformer;
import org.elasticsearch.indices.IndexTemplateAlreadyExistsException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.indices.InvalidIndexTemplateException;
import org.elasticsearch.indices.recovery.RecoverFilesRecoveryException;
import org.elasticsearch.percolator.PercolateException;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.admin.indices.alias.delete.AliasesNotFoundException;
import org.elasticsearch.search.SearchContextMissingException;
import org.elasticsearch.search.SearchException;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.warmer.IndexWarmerMissingException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.ActionTransportException;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.Set;

public class ExceptionSerializationTests extends ESTestCase {

    public void testExceptionRegistration()
            throws ClassNotFoundException, IOException, URISyntaxException {
        final Set<Class> notRegistered = new HashSet<>();
        final Set<Class> hasDedicatedWrite = new HashSet<>();
        final Set<String> registered = new HashSet<>();
        final String path = "/org/elasticsearch";
        final Path startPath = PathUtils.get(ElasticsearchException.class.getProtectionDomain().getCodeSource().getLocation().toURI()).resolve("org").resolve("elasticsearch");
        final Set<? extends Class> ignore = Sets.newHashSet(
                org.elasticsearch.test.rest.parser.RestTestParseException.class,
                org.elasticsearch.index.query.TestQueryParsingException.class,
                org.elasticsearch.test.rest.client.RestException.class,
                CancellableThreadsTests.CustomException.class,
                org.elasticsearch.rest.BytesRestResponseTests.WithHeadersException.class,
                AbstractClientHeadersTestCase.InternalException.class);
        FileVisitor<Path> visitor = new FileVisitor<Path>() {
            private Path pkgPrefix = PathUtils.get(path).getParent();

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path next = pkgPrefix.resolve(dir.getFileName());
                if (ignore.contains(next)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                pkgPrefix = next;
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                try {
                    String filename = file.getFileName().toString();
                    if (filename.endsWith(".class")) {
                        Class<?> clazz = loadClass(filename);
                        if (ignore.contains(clazz) == false) {
                            if (Modifier.isAbstract(clazz.getModifiers()) == false && Modifier.isInterface(clazz.getModifiers()) == false && isEsException(clazz)) {
                                if (ElasticsearchException.isRegistered(clazz.getName()) == false && ElasticsearchException.class.equals(clazz.getEnclosingClass()) == false) {
                                    notRegistered.add(clazz);
                                } else if (ElasticsearchException.isRegistered(clazz.getName())) {
                                    registered.add(clazz.getName());
                                    try {
                                        if (clazz.getDeclaredMethod("writeTo", StreamOutput.class) != null) {
                                            hasDedicatedWrite.add(clazz);
                                        }
                                    } catch (Exception e) {
                                        // fair enough
                                    }

                                }
                            }
                        }

                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                return FileVisitResult.CONTINUE;
            }

            private boolean isEsException(Class<?> clazz) {
                return ElasticsearchException.class.isAssignableFrom(clazz);
            }

            private Class<?> loadClass(String filename) throws ClassNotFoundException {
                StringBuilder pkg = new StringBuilder();
                for (Path p : pkgPrefix) {
                    pkg.append(p.getFileName().toString()).append(".");
                }
                pkg.append(filename.substring(0, filename.length() - 6));
                return getClass().getClassLoader().loadClass(pkg.toString());
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                throw exc;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                pkgPrefix = pkgPrefix.getParent();
                return FileVisitResult.CONTINUE;
            }
        };

        Files.walkFileTree(startPath, visitor);
        final Path testStartPath = PathUtils.get(ExceptionSerializationTests.class.getResource(path).toURI());
        Files.walkFileTree(testStartPath, visitor);
        assertTrue(notRegistered.remove(TestException.class));
        assertTrue(notRegistered.remove(UnknownHeaderException.class));
        assertTrue("Classes subclassing ElasticsearchException must be registered \n" + notRegistered.toString(),
                notRegistered.isEmpty());
        assertTrue(registered.removeAll(ElasticsearchException.getRegisteredKeys())); // check
        assertEquals(registered.toString(), 0, registered.size());
    }

    public static final class TestException extends ElasticsearchException {
        public TestException(StreamInput in) throws IOException{
            super(in);
        }
    }

    private <T extends Throwable> T serialize(T exception) throws IOException {
        ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersion(random()), exception);
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeThrowable(exception);
        StreamInput in = StreamInput.wrap(out.bytes());
        return in.readThrowable();
    }

    public void testIllegalShardRoutingStateException() throws IOException {
        final ShardRouting routing = TestShardRouting.newShardRouting("test", 0, "xyz", "def", false, ShardRoutingState.STARTED, 0);
        final String routingAsString = routing.toString();
        IllegalShardRoutingStateException serialize = serialize(new IllegalShardRoutingStateException(routing, "foo", new NullPointerException()));
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

    public void testQueryParsingException() throws IOException {
        QueryParsingException ex = serialize(new QueryParsingException(new Index("foo"), 1, 2, "fobar", null));
        assertEquals(ex.getIndex(), "foo");
        assertEquals(ex.getMessage(), "fobar");
        assertEquals(ex.getLineNumber(),1);
        assertEquals(ex.getColumnNumber(), 2);

        ex = serialize(new QueryParsingException(null, 1, 2, null, null));
        assertNull(ex.getIndex());
        assertNull(ex.getMessage());
        assertEquals(ex.getLineNumber(),1);
        assertEquals(ex.getColumnNumber(), 2);
    }

    public void testSearchException() throws IOException {
        SearchShardTarget target = new SearchShardTarget("foo", "bar", 1);
        SearchException ex = serialize(new SearchException(target, "hello world"));
        assertEquals(target, ex.shard());
        assertEquals(ex.getMessage(), "hello world");

        ex = serialize(new SearchException(null, "hello world", new NullPointerException()));
        assertNull(ex.shard());
        assertEquals(ex.getMessage(), "hello world");
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testAlreadyExpiredException() throws IOException {
        AlreadyExpiredException alreadyExpiredException = serialize(new AlreadyExpiredException("index", "type", "id", 1, 2, 3));
        assertEquals("index", alreadyExpiredException.getIndex());
        assertEquals("type", alreadyExpiredException.type());
        assertEquals("id", alreadyExpiredException.id());
        assertEquals(2, alreadyExpiredException.ttl());
        assertEquals(1, alreadyExpiredException.timestamp());
        assertEquals(3, alreadyExpiredException.now());

        alreadyExpiredException = serialize(new AlreadyExpiredException(null, null, null, -1, -2, -3));
        assertNull(alreadyExpiredException.getIndex());
        assertNull(alreadyExpiredException.type());
        assertNull(alreadyExpiredException.id());
        assertEquals(-2, alreadyExpiredException.ttl());
        assertEquals(-1, alreadyExpiredException.timestamp());
        assertEquals(-3, alreadyExpiredException.now());
    }

    public void testCreateFailedEngineException() throws IOException {
        CreateFailedEngineException ex = serialize(new CreateFailedEngineException(new ShardId("idx", 2), "type", "id", null));
        assertEquals(ex.getShardId(), new ShardId("idx", 2));
        assertEquals("type", ex.type());
        assertEquals("id", ex.id());
        assertNull(ex.getCause());

        ex = serialize(new CreateFailedEngineException(null, "type", "id", new NullPointerException()));
        assertNull(ex.getShardId());
        assertEquals("type", ex.type());
        assertEquals("id", ex.id());
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testMergeMappingException() throws IOException {
        MergeMappingException ex = serialize(new MergeMappingException(new String[] {"one", "two"}));
        assertArrayEquals(ex.failures(), new String[]{"one", "two"});
    }

    public void testActionNotFoundTransportException() throws IOException {
        ActionNotFoundTransportException ex = serialize(new ActionNotFoundTransportException("AACCCTION"));
        assertEquals("AACCCTION", ex.action());
        assertEquals("No handler for action [AACCCTION]", ex.getMessage());
    }

    public void testSnapshotException() throws IOException {
        SnapshotException ex = serialize(new SnapshotException(new SnapshotId("repo", "snap"), "no such snapshot", new NullPointerException()));
        assertEquals(ex.snapshot(), new SnapshotId("repo", "snap"));
        assertEquals(ex.getMessage(), "[repo:snap] no such snapshot");
        assertTrue(ex.getCause() instanceof NullPointerException);

        ex = serialize(new SnapshotException(null, "no such snapshot", new NullPointerException()));
        assertEquals(ex.snapshot(), null);
        assertEquals(ex.getMessage(), "[_na] no such snapshot");
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testRecoverFilesRecoveryException() throws IOException {
        ShardId id = new ShardId("foo", 1);
        ByteSizeValue bytes = new ByteSizeValue(randomIntBetween(0, 10000));
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

    public void testIndexTemplateAlreadyExistsException() throws IOException {
        IndexTemplateAlreadyExistsException ex = serialize(new IndexTemplateAlreadyExistsException("the dude abides!"));
        assertEquals("the dude abides!", ex.name());
        assertEquals("index_template [the dude abides!] already exists", ex.getMessage());

        ex = serialize(new IndexTemplateAlreadyExistsException((String)null));
        assertNull(ex.name());
        assertEquals("index_template [null] already exists", ex.getMessage());
    }

    public void testBatchOperationException() throws IOException {
        ShardId id = new ShardId("foo", 1);
        TranslogRecoveryPerformer.BatchOperationException ex = serialize(new TranslogRecoveryPerformer.BatchOperationException(id, "batched the fucker", 666, null));
        assertEquals(ex.getShardId(), id);
        assertEquals(666, ex.completedOperations());
        assertEquals("batched the fucker", ex.getMessage());
        assertNull(ex.getCause());

        ex = serialize(new TranslogRecoveryPerformer.BatchOperationException(null, "batched the fucker", -1, new NullPointerException()));
        assertNull(ex.getShardId());
        assertEquals(-1, ex.completedOperations());
        assertEquals("batched the fucker", ex.getMessage());
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testInvalidIndexTemplateException() throws IOException {
        InvalidIndexTemplateException ex = serialize(new InvalidIndexTemplateException("foo", "bar"));
        assertEquals(ex.getMessage(), "index_template [foo] invalid, cause [bar]");
        assertEquals(ex.name(), "foo");
        ex = serialize(new InvalidIndexTemplateException(null, "bar"));
        assertEquals(ex.getMessage(), "index_template [null] invalid, cause [bar]");
        assertEquals(ex.name(), null);
    }

    public void testActionTransportException() throws IOException {
        ActionTransportException ex = serialize(new ActionTransportException("name?", new LocalTransportAddress("dead.end:666"), "ACTION BABY!", "message?", null));
        assertEquals("ACTION BABY!", ex.action());
        assertEquals(new LocalTransportAddress("dead.end:666"), ex.address());
        assertEquals("[name?][local[dead.end:666]][ACTION BABY!] message?", ex.getMessage());
    }

    public void testSearchContextMissingException() throws IOException {
        long id = randomLong();
        SearchContextMissingException ex = serialize(new SearchContextMissingException(id));
        assertEquals(id, ex.id());
    }

    public void testPercolateException() throws IOException {
        ShardId id = new ShardId("foo", 1);
        PercolateException ex = serialize(new PercolateException(id, "percolate my ass", null));
        assertEquals(id, ex.getShardId());
        assertEquals("percolate my ass", ex.getMessage());
        assertNull(ex.getCause());

        ex = serialize(new PercolateException(id, "percolate my ass", new NullPointerException()));
        assertEquals(id, ex.getShardId());
        assertEquals("percolate my ass", ex.getMessage());
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testRoutingValidationException() throws IOException {
        RoutingTableValidation validation = new RoutingTableValidation();
        validation.addIndexFailure("foo", "bar");
        RoutingValidationException ex = serialize(new RoutingValidationException(validation));
        assertEquals("[Index [foo]: bar]", ex.getMessage());
        assertEquals(validation.toString(), ex.validation().toString());
    }

    public void testCircuitBreakingException() throws IOException {
        CircuitBreakingException ex = serialize(new CircuitBreakingException("I hate to say I told you so...", 0, 100));
        assertEquals("I hate to say I told you so...", ex.getMessage());
        assertEquals(100, ex.getByteLimit());
        assertEquals(0, ex.getBytesWanted());
    }

    public void testTimestampParsingException() throws IOException {
        TimestampParsingException ex = serialize(new TimestampParsingException("TIMESTAMP", null));
        assertEquals("failed to parse timestamp [TIMESTAMP]", ex.getMessage());
        assertEquals("TIMESTAMP", ex.timestamp());
    }

    public void testIndexFailedEngineException() throws IOException {
        ShardId id = new ShardId("foo", 1);
        IndexFailedEngineException ex = serialize(new IndexFailedEngineException(id, "type", "id", null));
        assertEquals(ex.getShardId(), new ShardId("foo", 1));
        assertEquals("type", ex.type());
        assertEquals("id", ex.id());
        assertNull(ex.getCause());

        ex = serialize(new IndexFailedEngineException(null, "type", "id", new NullPointerException()));
        assertNull(ex.getShardId());
        assertEquals("type", ex.type());
        assertEquals("id", ex.id());
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testAliasesMissingException() throws IOException {
        AliasesNotFoundException ex = serialize(new AliasesNotFoundException("one", "two", "three"));
        assertEquals("aliases [one, two, three] missing", ex.getMessage());
        assertEquals("aliases", ex.getResourceType());
        assertArrayEquals(new String[]{"one", "two", "three"}, ex.getResourceId().toArray(new String[0]));
    }

    public void testSearchParseException() throws IOException {
        SearchContext ctx = new TestSearchContext();
        SearchParseException ex = serialize(new SearchParseException(ctx, "foo", new XContentLocation(66, 666)));
        assertEquals("foo", ex.getMessage());
        assertEquals(66, ex.getLineNumber());
        assertEquals(666, ex.getColumnNumber());
        assertEquals(ctx.shardTarget(), ex.shard());
    }

    public void testIllegalIndexShardStateException()throws IOException {
        ShardId id = new ShardId("foo", 1);
        IndexShardState state = randomFrom(IndexShardState.values());
        IllegalIndexShardStateException ex = serialize(new IllegalIndexShardStateException(id, state, "come back later buddy"));
        assertEquals(id, ex.getShardId());
        assertEquals("CurrentState[" + state.name() + "] come back later buddy", ex.getMessage());
        assertEquals(state, ex.currentState());
    }

    public void testConnectTransportException() throws IOException {
        DiscoveryNode node = new DiscoveryNode("thenode", new LocalTransportAddress("dead.end:666"), Version.CURRENT);
        ConnectTransportException ex = serialize(new ConnectTransportException(node, "msg", "action", null));
        assertEquals("[][local[dead.end:666]][action] msg", ex.getMessage());
        assertEquals(node, ex.node());
        assertEquals("action", ex.action());
        assertNull(ex.getCause());

        ex = serialize(new ConnectTransportException(node, "msg", "action", new NullPointerException()));
        assertEquals("[][local[dead.end:666]][action] msg", ex.getMessage());
        assertEquals(node, ex.node());
        assertEquals("action", ex.action());
        assertTrue(ex.getCause() instanceof NullPointerException);
    }

    public void testSearchPhaseExecutionException() throws IOException {
        ShardSearchFailure[] empty = new ShardSearchFailure[0];
        SearchPhaseExecutionException ex = serialize(new SearchPhaseExecutionException("boom", "baam", new NullPointerException(), empty));
        assertEquals("boom", ex.getPhaseName());
        assertEquals("baam", ex.getMessage());
        assertTrue(ex.getCause() instanceof NullPointerException);
        assertEquals(empty.length, ex.shardFailures().length);
        ShardSearchFailure[] one = new ShardSearchFailure[] {
                new ShardSearchFailure(new IllegalArgumentException("nono!"))
        };

        ex = serialize(new SearchPhaseExecutionException("boom", "baam", new NullPointerException(), one));
        assertEquals("boom", ex.getPhaseName());
        assertEquals("baam", ex.getMessage());
        assertTrue(ex.getCause() instanceof NullPointerException);
        assertEquals(one.length, ex.shardFailures().length);
        assertTrue(ex.shardFailures()[0].getCause() instanceof IllegalArgumentException);
    }

    public void testRoutingMissingException() throws IOException {
        RoutingMissingException ex = serialize(new RoutingMissingException("idx", "type", "id"));
        assertEquals("idx", ex.getIndex());
        assertEquals("type", ex.getType());
        assertEquals("id", ex.getId());
        assertEquals("routing is required for [idx]/[type]/[id]", ex.getMessage());
    }

    public void testRepositoryException() throws IOException {
        RepositoryException ex = serialize(new RepositoryException("repo", "msg"));
        assertEquals("repo", ex.repository());
        assertEquals("[repo] msg", ex.getMessage());

        ex = serialize(new RepositoryException(null, "msg"));
        assertNull(ex.repository());
        assertEquals("[_na] msg", ex.getMessage());
    }

    public void testIndexWarmerMissingException() throws IOException {
        IndexWarmerMissingException ex = serialize(new IndexWarmerMissingException("w1", "w2"));
        assertEquals("index_warmer [w1, w2] missing", ex.getMessage());
        assertArrayEquals(new String[]{"w1", "w2"}, ex.names());
    }

    public void testIndexTemplateMissingException() throws IOException {
        IndexTemplateMissingException ex = serialize(new IndexTemplateMissingException("name"));
        assertEquals("index_template [name] missing", ex.getMessage());
        assertEquals("name", ex.name());

        ex = serialize(new IndexTemplateMissingException((String)null));
        assertEquals("index_template [null] missing", ex.getMessage());
        assertNull(ex.name());
    }


    public void testRecoveryEngineException() throws IOException {
        ShardId id = new ShardId("foo", 1);
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
        ClusterBlockException ex = serialize(new ClusterBlockException(ImmutableSet.of(DiscoverySettings.NO_MASTER_BLOCK_WRITES)));
        assertEquals("blocked by: [SERVICE_UNAVAILABLE/2/no master];", ex.getMessage());
        assertTrue(ex.blocks().contains(DiscoverySettings.NO_MASTER_BLOCK_WRITES));
        assertEquals(1, ex.blocks().size());
    }

    private String toXContent(ToXContent x) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            x.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    public void testNotSerializableExceptionWrapper() throws IOException {
        NotSerializableExceptionWrapper ex = serialize(new NotSerializableExceptionWrapper(new NullPointerException()));
        assertEquals("{\"type\":\"null_pointer_exception\",\"reason\":null}", toXContent(ex));
        ex = serialize(new NotSerializableExceptionWrapper(new IllegalArgumentException("nono!")));
        assertEquals("{\"type\":\"illegal_argument_exception\",\"reason\":\"nono!\"}", toXContent(ex));

        Throwable[] unknowns = new Throwable[] {
                new JsonParseException("foobar", new JsonLocation(new Object(), 1,2,3,4)),
                new GroovyCastException("boom boom boom"),
                new IOException("booom")
        };
        for (Throwable t : unknowns) {
            if (randomBoolean()) {
                t.addSuppressed(new IOException("suppressed"));
                t.addSuppressed(new NullPointerException());
            }
            Throwable deserialized = serialize(t);
            assertTrue(deserialized instanceof NotSerializableExceptionWrapper);
            // TODO: fix this test for more java 9 differences
            if (!Constants.JRE_IS_MINIMUM_JAVA9) {
                assertArrayEquals(t.getStackTrace(), deserialized.getStackTrace());
                assertEquals(t.getSuppressed().length, deserialized.getSuppressed().length);
                if (t.getSuppressed().length > 0) {
                    assertTrue(deserialized.getSuppressed()[0] instanceof NotSerializableExceptionWrapper);
                    assertArrayEquals(t.getSuppressed()[0].getStackTrace(), deserialized.getSuppressed()[0].getStackTrace());
                    assertTrue(deserialized.getSuppressed()[1] instanceof NullPointerException);
                }
            }
        }
    }

    public void testWithRestHeadersException() throws IOException {
        ElasticsearchException ex = new ElasticsearchException("msg");
        ex.addHeader("foo", "foo", "bar");
        ex = serialize(ex);
        assertEquals("msg", ex.getMessage());
        assertEquals(2, ex.getHeader("foo").size());
        assertEquals("foo", ex.getHeader("foo").get(0));
        assertEquals("bar", ex.getHeader("foo").get(1));

        RestStatus status = randomFrom(RestStatus.values());
        // ensure we are carrying over the headers even if not serialized
        UnknownHeaderException uhe = new UnknownHeaderException("msg", status);
        uhe.addHeader("foo", "foo", "bar");

        ElasticsearchException serialize = serialize((ElasticsearchException)uhe);
        assertTrue(serialize instanceof NotSerializableExceptionWrapper);
        NotSerializableExceptionWrapper e = (NotSerializableExceptionWrapper) serialize;
        assertEquals("msg", e.getMessage());
        assertEquals(2, e.getHeader("foo").size());
        assertEquals("foo", e.getHeader("foo").get(0));
        assertEquals("bar", e.getHeader("foo").get(1));
        assertSame(status, e.status());

    }

    public static class UnknownHeaderException extends ElasticsearchException {
        private final RestStatus status;

        public UnknownHeaderException(String msg, RestStatus status) {
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

    public static class UnknownException extends Exception {
        public UnknownException(String message) {
            super(message);
        }

        public UnknownException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
