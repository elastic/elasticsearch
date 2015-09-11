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
import org.apache.lucene.util.Constants;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.TestQueryParsingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.transport.RemoteTransportException;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class ESExceptionTests extends ESTestCase {
    private static final ToXContent.Params PARAMS = ToXContent.EMPTY_PARAMS;

    @Test
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
            ShardSearchFailure failure = new ShardSearchFailure(new TestQueryParsingException(new Index("foo"), "foobar", null),
                    new SearchShardTarget("node_1", "foo", 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new TestQueryParsingException(new Index("foo"), "foobar", null),
                    new SearchShardTarget("node_1", "foo", 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1});
            if (randomBoolean()) {
                rootCauses = (randomBoolean() ? new RemoteTransportException("remoteboom", ex) : ex).guessRootCauses();
            } else {
                rootCauses = ElasticsearchException.guessRootCauses(randomBoolean() ? new RemoteTransportException("remoteboom", ex) : ex);
            }
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "test_query_parsing_exception");
            assertEquals(rootCauses[0].getMessage(), "foobar");

            ElasticsearchException oneLevel = new ElasticsearchException("foo", new RuntimeException("foobar"));
            rootCauses = oneLevel.guessRootCauses();
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "exception");
            assertEquals(rootCauses[0].getMessage(), "foo");
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(
                    new TestQueryParsingException(new Index("foo"), 1, 2, "foobar", null),
                    new SearchShardTarget("node_1", "foo", 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new TestQueryParsingException(new Index("foo1"), 1, 2, "foobar", null),
                    new SearchShardTarget("node_1", "foo1", 1));
            ShardSearchFailure failure2 = new ShardSearchFailure(new TestQueryParsingException(new Index("foo1"), 1, 2, "foobar", null),
                    new SearchShardTarget("node_1", "foo1", 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1, failure2});
            final ElasticsearchException[] rootCauses = ex.guessRootCauses();
            assertEquals(rootCauses.length, 2);
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "test_query_parsing_exception");
            assertEquals(rootCauses[0].getMessage(), "foobar");
            assertEquals(((QueryParsingException)rootCauses[0]).getIndex(), "foo");
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[1]), "test_query_parsing_exception");
            assertEquals(rootCauses[1].getMessage(), "foobar");
            assertEquals(((QueryParsingException) rootCauses[1]).getLineNumber(), 1);
            assertEquals(((QueryParsingException) rootCauses[1]).getColumnNumber(), 2);

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
            ShardSearchFailure failure = new ShardSearchFailure(new TestQueryParsingException(new Index("foo"), "foobar", null),
                    new SearchShardTarget("node_1", "foo", 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new TestQueryParsingException(new Index("foo"), "foobar", null),
                    new SearchShardTarget("node_1", "foo", 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"test_query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo\"}}]}";
            assertEquals(expected, builder.string());
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new TestQueryParsingException(new Index("foo"), "foobar", null),
                    new SearchShardTarget("node_1", "foo", 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new TestQueryParsingException(new Index("foo1"), "foobar", null),
                    new SearchShardTarget("node_1", "foo1", 1));
            ShardSearchFailure failure2 = new ShardSearchFailure(new TestQueryParsingException(new Index("foo1"), "foobar", null),
                    new SearchShardTarget("node_1", "foo1", 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1, failure2});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"test_query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo\"}},{\"shard\":1,\"index\":\"foo1\",\"node\":\"node_1\",\"reason\":{\"type\":\"test_query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo1\"}}]}";
            assertEquals(expected, builder.string());
        }
    }

    public void testGetRootCause() {
        Exception root = new RuntimeException("foobar");
        ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar", new IllegalArgumentException("index is closed", root)));
        assertEquals(root, exception.getRootCause());
        assertTrue(exception.contains(RuntimeException.class));
        assertFalse(exception.contains(EOFException.class));
    }

    public void testToString() {
        ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar", new IllegalArgumentException("index is closed", new RuntimeException("foobar"))));
        assertEquals("ElasticsearchException[foo]; nested: ElasticsearchException[bar]; nested: IllegalArgumentException[index is closed]; nested: RuntimeException[foobar];", exception.toString());
    }

    public void testToXContent() throws IOException {
        {
            ElasticsearchException ex = new SearchParseException(new TestSearchContext(), "foo", new XContentLocation(1,0));
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
            QueryParsingException ex = new TestQueryParsingException(new Index("foo"), 1, 2, "foobar", null);
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ElasticsearchException.toXContent(builder, PARAMS, ex);
            builder.endObject();
            String expected = "{\"type\":\"test_query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo\",\"line\":1,\"col\":2}";
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
            QueryParsingException ex = new TestQueryParsingException(new Index("foo"), 1, 2, "foobar", null);
            ex.addHeader("test", "some value");
            ex.addHeader("test_multi", "some value", "another value");
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ElasticsearchException.toXContent(builder, PARAMS, ex);
            builder.endObject();
            assertThat(builder.string(), Matchers.anyOf( // iteration order depends on platform
                    equalTo("{\"type\":\"test_query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo\",\"line\":1,\"col\":2,\"header\":{\"test_multi\":[\"some value\",\"another value\"],\"test\":\"some value\"}}"),
                    equalTo("{\"type\":\"test_query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo\",\"line\":1,\"col\":2,\"header\":{\"test\":\"some value\",\"test_multi\":[\"some value\",\"another value\"]}}")
            ));
        }
    }

    public void testSerializeElasticsearchException() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        QueryParsingException ex = new QueryParsingException(new Index("foo"), 1, 2, "foobar", null);
        out.writeThrowable(ex);

        StreamInput in = StreamInput.wrap(out.bytes());
        QueryParsingException e = in.readThrowable();
        assertEquals(ex.getIndex(), e.getIndex());
        assertEquals(ex.getMessage(), e.getMessage());
        assertEquals(ex.getLineNumber(), e.getLineNumber());
        assertEquals(ex.getColumnNumber(), e.getColumnNumber());
    }

    public void testSerializeUnknownException() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        QueryParsingException queryParsingException = new QueryParsingException(new Index("foo"), 1, 2, "foobar", null);
        Throwable ex = new Throwable("wtf", queryParsingException);
        out.writeThrowable(ex);

        StreamInput in = StreamInput.wrap(out.bytes());
        Throwable throwable = in.readThrowable();
        assertEquals("wtf", throwable.getMessage());
        assertTrue(throwable instanceof ElasticsearchException);
        QueryParsingException e = (QueryParsingException)throwable.getCause();
                assertEquals(queryParsingException.getIndex(), e.getIndex());
        assertEquals(queryParsingException.getMessage(), e.getMessage());
        assertEquals(queryParsingException.getLineNumber(), e.getLineNumber());
        assertEquals(queryParsingException.getColumnNumber(), e.getColumnNumber());
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
                new Throwable("this exception is unknown", new QueryParsingException(new Index("foo"), 1, 2, "foobar", null) ), // somethin unknown
        };
        for (Throwable t : causes) {
            BytesStreamOutput out = new BytesStreamOutput();
            ElasticsearchException ex = new ElasticsearchException("topLevel", t);
            out.writeThrowable(ex);
            StreamInput in = StreamInput.wrap(out.bytes());
            ElasticsearchException e = in.readThrowable();
            assertEquals(e.getMessage(), ex.getMessage());
            assertEquals(ex.getCause().getClass().getName(), e.getCause().getMessage(), ex.getCause().getMessage());
            if (ex.getCause().getClass() != Throwable.class) { // throwable is not directly mapped
                assertEquals(e.getCause().getClass(), ex.getCause().getClass());
            } else {
                assertEquals(e.getCause().getClass(), NotSerializableExceptionWrapper.class);
            }
            // TODO: fix this test
            // on java 9, expected:<sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)> 
            //            but was:<sun.reflect.NativeMethodAccessorImpl.invoke0(java.base@9.0/Native Method)>
            if (!Constants.JRE_IS_MINIMUM_JAVA9) {
                assertArrayEquals(e.getStackTrace(), ex.getStackTrace());
            }
            assertTrue(e.getStackTrace().length > 1);
            ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersion(getRandom()), t);
            ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersion(getRandom()), ex);
            ElasticsearchAssertions.assertVersionSerializable(VersionUtils.randomVersion(getRandom()), e);
        }
    }
}