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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.transport.RemoteTransportException;
import org.hamcrest.Matchers;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static org.hamcrest.Matchers.equalTo;

public class ESExceptionTests extends ESTestCase {
    private static final ToXContent.Params PARAMS = ToXContent.EMPTY_PARAMS;

    public void testStatus() {
        ElasticsearchException exception = new ElasticsearchException("test");
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new ElasticsearchException("test", new RuntimeException());
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new ElasticsearchException("test", new ResourceNotFoundException("test"));
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new RemoteTransportException("test", new ResourceNotFoundException("test"));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));

        exception = new RemoteTransportException("test", new IllegalArgumentException("foobar"));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));

        exception = new RemoteTransportException("test", new IllegalStateException("foobar"));
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testGuessRootCause() {
        {
            ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar", new IndexNotFoundException("foo", new RuntimeException("foobar"))));
            ElasticsearchException[] rootCauses = exception.guessRootCauses();
            assertEquals(rootCauses.length, 1);
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "index_not_found_exception");
            assertEquals(rootCauses[0].getMessage(), "no such index");
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1});
            if (randomBoolean()) {
                rootCauses = (randomBoolean() ? new RemoteTransportException("remoteboom", ex) : ex).guessRootCauses();
            } else {
                rootCauses = ElasticsearchException.guessRootCauses(randomBoolean() ? new RemoteTransportException("remoteboom", ex) : ex);
            }
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "parsing_exception");
            assertEquals(rootCauses[0].getMessage(), "foobar");

            ElasticsearchException oneLevel = new ElasticsearchException("foo", new RuntimeException("foobar"));
            rootCauses = oneLevel.guessRootCauses();
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "exception");
            assertEquals(rootCauses[0].getMessage(), "foo");
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(
                    new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo1", "_na_"), 1));
            ShardSearchFailure failure2 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo1", "_na_"), 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1, failure2});
            final ElasticsearchException[] rootCauses = ex.guessRootCauses();
            assertEquals(rootCauses.length, 2);
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "parsing_exception");
            assertEquals(rootCauses[0].getMessage(), "foobar");
            assertEquals(((ParsingException) rootCauses[0]).getLineNumber(), 1);
            assertEquals(((ParsingException) rootCauses[0]).getColumnNumber(), 2);
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[1]), "query_shard_exception");
            assertEquals((rootCauses[1]).getIndex().getName(), "foo1");
            assertEquals(rootCauses[1].getMessage(), "foobar");
        }

        {
            final ElasticsearchException[] foobars = ElasticsearchException.guessRootCauses(new IllegalArgumentException("foobar"));
            assertEquals(foobars.length, 1);
            assertTrue(foobars[0] instanceof ElasticsearchException);
            assertEquals(foobars[0].getMessage(), "foobar");
            assertEquals(foobars[0].getCause().getClass(), IllegalArgumentException.class);
            assertEquals(foobars[0].getExceptionName(), "illegal_argument_exception");
        }

    }

    public void testDeduplicate() throws IOException {
        {
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", randomBoolean() ? failure1.getCause() : failure.getCause(), new ShardSearchFailure[]{failure, failure1});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}}]}";
            assertEquals(expected, builder.string());
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo1", "_na_"), 1));
            ShardSearchFailure failure2 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo1", "_na_"), 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1, failure2});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}},{\"shard\":1,\"index\":\"foo1\",\"node\":\"node_1\",\"reason\":{\"type\":\"query_shard_exception\",\"reason\":\"foobar\",\"index_uuid\":\"_na_\",\"index\":\"foo1\"}}]}";
            assertEquals(expected, builder.string());
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 2));
            NullPointerException nullPointerException = new NullPointerException();
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", nullPointerException, new ShardSearchFailure[]{failure, failure1});
            assertEquals(nullPointerException, ex.getCause());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}}],\"caused_by\":{\"type\":\"null_pointer_exception\",\"reason\":null}}";
            assertEquals(expected, builder.string());
        }
    }

    /**
     * Check whether this exception contains an exception of the given type:
     * either it is of the given class itself or it contains a nested cause
     * of the given type.
     *
     * @param exType the exception type to look for
     * @return whether there is a nested exception of the specified type
     */
    private boolean contains(Throwable t, Class<? extends Throwable> exType) {
        if (exType == null) {
            return false;
        }
        for (Throwable cause = t; t != null; t = t.getCause()) {
            if (exType.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }

    public void testGetRootCause() {
        Exception root = new RuntimeException("foobar");
        ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar", new IllegalArgumentException("index is closed", root)));
        assertEquals(root, exception.getRootCause());
        assertTrue(contains(exception, RuntimeException.class));
        assertFalse(contains(exception, EOFException.class));
    }

    public void testToString() {
        ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar", new IllegalArgumentException("index is closed", new RuntimeException("foobar"))));
        assertEquals("ElasticsearchException[foo]; nested: ElasticsearchException[bar]; nested: IllegalArgumentException[index is closed]; nested: RuntimeException[foobar];", exception.toString());
    }

    public void testToXContent() throws IOException {
        {
            ElasticsearchException ex = new SearchParseException(new TestSearchContext(null), "foo", new XContentLocation(1,0));
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, PARAMS);
            builder.endObject();

            String expected = "{\"type\":\"search_parse_exception\",\"reason\":\"foo\",\"line\":1,\"col\":0}";
            assertEquals(expected, builder.string());
        }
        {
            ElasticsearchException ex = new ElasticsearchException("foo", new ElasticsearchException("bar", new IllegalArgumentException("index is closed", new RuntimeException("foobar"))));
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, PARAMS);
            builder.endObject();

            String expected = "{\"type\":\"exception\",\"reason\":\"foo\",\"caused_by\":{\"type\":\"exception\",\"reason\":\"bar\",\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"index is closed\",\"caused_by\":{\"type\":\"runtime_exception\",\"reason\":\"foobar\"}}}}";
            assertEquals(expected, builder.string());
        }

        {
            Exception ex = new FileNotFoundException("foo not found");
            if (randomBoolean()) {
                // just a wrapper which is omitted
                ex = new RemoteTransportException("foobar", ex);
            }
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ElasticsearchException.toXContent(builder, PARAMS, ex);
            builder.endObject();

            String expected = "{\"type\":\"file_not_found_exception\",\"reason\":\"foo not found\"}";
            assertEquals(expected, builder.string());
        }

        {
            ParsingException ex = new ParsingException(1, 2, "foobar", null);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ElasticsearchException.toXContent(builder, PARAMS, ex);
            builder.endObject();
            String expected = "{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}";
            assertEquals(expected, builder.string());
        }

        { // test equivalence
            ElasticsearchException ex =  new RemoteTransportException("foobar", new FileNotFoundException("foo not found"));
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ElasticsearchException.toXContent(builder, PARAMS, ex);
            builder.endObject();

            XContentBuilder otherBuilder = XContentFactory.jsonBuilder();

            otherBuilder.startObject();
            ex.toXContent(otherBuilder, PARAMS);
            otherBuilder.endObject();
            assertEquals(otherBuilder.string(), builder.string());
            assertEquals("{\"type\":\"file_not_found_exception\",\"reason\":\"foo not found\"}", builder.string());
        }

        { // render header
            ParsingException ex = new ParsingException(1, 2, "foobar", null);
            ex.addHeader("test", "some value");
            ex.addHeader("test_multi", "some value", "another value");
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ElasticsearchException.toXContent(builder, PARAMS, ex);
            builder.endObject();
            assertThat(builder.string(), Matchers.anyOf( // iteration order depends on platform
                            equalTo("{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2,\"header\":{\"test_multi\":[\"some value\",\"another value\"],\"test\":\"some value\"}}"),
                            equalTo("{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2,\"header\":{\"test\":\"some value\",\"test_multi\":[\"some value\",\"another value\"]}}")
            ));
        }
    }

    public void testSerializeElasticsearchException() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        ParsingException ex = new ParsingException(1, 2, "foobar", null);
        out.writeThrowable(ex);

        StreamInput in = StreamInput.wrap(out.bytes());
        ParsingException e = in.readThrowable();
        assertEquals(ex.getIndex(), e.getIndex());
        assertEquals(ex.getMessage(), e.getMessage());
        assertEquals(ex.getLineNumber(), e.getLineNumber());
        assertEquals(ex.getColumnNumber(), e.getColumnNumber());
    }

    public void testSerializeUnknownException() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        ParsingException ParsingException = new ParsingException(1, 2, "foobar", null);
        Throwable ex = new Throwable("eggplant", ParsingException);
        out.writeThrowable(ex);

        StreamInput in = StreamInput.wrap(out.bytes());
        Throwable throwable = in.readThrowable();
        assertEquals("throwable: eggplant", throwable.getMessage());
        assertTrue(throwable instanceof ElasticsearchException);
        ParsingException e = (ParsingException)throwable.getCause();
                assertEquals(ParsingException.getIndex(), e.getIndex());
        assertEquals(ParsingException.getMessage(), e.getMessage());
        assertEquals(ParsingException.getLineNumber(), e.getLineNumber());
        assertEquals(ParsingException.getColumnNumber(), e.getColumnNumber());
    }

    public void testWriteThrowable() throws IOException {
        Throwable[] causes = new Throwable[] {
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
                new AssertionError("booom", new NullPointerException()),
                new OutOfMemoryError("no memory left"),
                new AlreadyClosedException("closed!!", new NullPointerException()),
                new LockObtainFailedException("can't lock directory", new NullPointerException()),
                new Throwable("this exception is unknown", new QueryShardException(new Index("foo", "_na_"), "foobar", null) ), // somethin unknown
        };
        for (Throwable t : causes) {
            BytesStreamOutput out = new BytesStreamOutput();
            ElasticsearchException ex = new ElasticsearchException("topLevel", t);
            out.writeThrowable(ex);
            StreamInput in = StreamInput.wrap(out.bytes());
            ElasticsearchException e = in.readThrowable();
            assertEquals(e.getMessage(), ex.getMessage());
            assertTrue("Expected: " + e.getCause().getMessage() + " to contain: " +
                            ex.getCause().getClass().getName() + " but it didn't",
                    e.getCause().getMessage().contains(ex.getCause().getMessage()));
            if (ex.getCause().getClass() != Throwable.class) { // throwable is not directly mapped
                assertEquals(e.getCause().getClass(), ex.getCause().getClass());
            } else {
                assertEquals(e.getCause().getClass(), NotSerializableExceptionWrapper.class);
            }
            assertArrayEquals(e.getStackTrace(), ex.getStackTrace());
            assertTrue(e.getStackTrace().length > 1);
            ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersion(random()), t);
            ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersion(random()), ex);
            ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersion(random()), e);
        }
    }
}
