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

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.RemoteTransportException;
import org.junit.Test;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ElasticsearchExceptionTests extends ElasticsearchTestCase {

    @Test
    public void testStatus() {
        ElasticsearchException exception = new ElasticsearchException("test");
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new ElasticsearchException("test", new RuntimeException());
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new ElasticsearchException("test", new IndexMissingException(new Index("test")));
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new RemoteTransportException("test", new IndexMissingException(new Index("test")));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));

        exception = new RemoteTransportException("test", new IllegalArgumentException("foobar"));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));

        exception = new RemoteTransportException("test", new IllegalStateException("foobar"));
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testGuessRootCause() {
        {
            ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar", new ElasticsearchIllegalArgumentException("index is closed", new RuntimeException("foobar"))));
            ElasticsearchException[] rootCauses = exception.guessRootCauses();
            assertEquals(rootCauses.length, 1);
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "illegal_argument_exception");
            assertEquals(rootCauses[0].getMessage(), "index is closed");
            ShardSearchFailure failure = new ShardSearchFailure(new QueryParsingException(new Index("foo"), "foobar"), new SearchShardTarget("node_1", "foo", 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryParsingException(new Index("foo"), "foobar"), new SearchShardTarget("node_1", "foo", 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1});
            if (randomBoolean()) {
                rootCauses = (randomBoolean() ? new RemoteTransportException("remoteboom", ex) : ex).guessRootCauses();
            } else {
                rootCauses = ElasticsearchException.guessRootCauses(randomBoolean() ? new RemoteTransportException("remoteboom", ex) : ex);
            }
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "query_parsing_exception");
            assertEquals(rootCauses[0].getMessage(), "foobar");

            ElasticsearchException oneLevel = new ElasticsearchException("foo", new RuntimeException("foobar"));
            rootCauses = oneLevel.guessRootCauses();
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "exception");
            assertEquals(rootCauses[0].getMessage(), "foo");
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new QueryParsingException(new Index("foo"), "foobar"), new SearchShardTarget("node_1", "foo", 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryParsingException(new Index("foo1"), "foobar"), new SearchShardTarget("node_1", "foo1", 1));
            ShardSearchFailure failure2 = new ShardSearchFailure(new QueryParsingException(new Index("foo1"), "foobar"), new SearchShardTarget("node_1", "foo1", 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1, failure2});
            final ElasticsearchException[] rootCauses = ex.guessRootCauses();
            assertEquals(rootCauses.length, 2);
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "query_parsing_exception");
            assertEquals(rootCauses[0].getMessage(), "foobar");
            assertEquals(((QueryParsingException)rootCauses[0]).index().name(), "foo");
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[1]), "query_parsing_exception");
            assertEquals(rootCauses[1].getMessage(), "foobar");
            assertEquals(((QueryParsingException)rootCauses[1]).index().name(), "foo1");

        }

    }

    public void testDeduplicate() throws IOException {
        {
            ShardSearchFailure failure = new ShardSearchFailure(new QueryParsingException(new Index("foo"), "foobar"), new SearchShardTarget("node_1", "foo", 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryParsingException(new Index("foo"), "foobar"), new SearchShardTarget("node_1", "foo", 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo\"}}]}";
            assertEquals(expected, builder.string());
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new QueryParsingException(new Index("foo"), "foobar"), new SearchShardTarget("node_1", "foo", 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryParsingException(new Index("foo1"), "foobar"), new SearchShardTarget("node_1", "foo1", 1));
            ShardSearchFailure failure2 = new ShardSearchFailure(new QueryParsingException(new Index("foo1"), "foobar"), new SearchShardTarget("node_1", "foo1", 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", new ShardSearchFailure[]{failure, failure1, failure2});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":{\"type\":\"query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo\"}},{\"shard\":1,\"index\":\"foo1\",\"node\":\"node_1\",\"reason\":{\"type\":\"query_parsing_exception\",\"reason\":\"foobar\",\"index\":\"foo1\"}}]}";
            assertEquals(expected, builder.string());
        }
    }

    public void testGetRootCause() {
        Exception root = new RuntimeException("foobar");
        ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar", new ElasticsearchIllegalArgumentException("index is closed", root)));
        assertEquals(root, exception.getRootCause());
        assertTrue(exception.contains(RuntimeException.class));
        assertFalse(exception.contains(EOFException.class));
    }

    public void testToString() {
        ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar", new ElasticsearchIllegalArgumentException("index is closed", new RuntimeException("foobar"))));
        assertEquals("ElasticsearchException[foo]; nested: ElasticsearchException[bar]; nested: ElasticsearchIllegalArgumentException[index is closed]; nested: RuntimeException[foobar];", exception.toString());
    }

    public void testToXContent() throws IOException {
        {
            ElasticsearchException ex = new ElasticsearchException("foo", new ElasticsearchException("bar", new ElasticsearchIllegalArgumentException("index is closed", new RuntimeException("foobar"))));
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
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
            ElasticsearchException.toXContent(builder, ToXContent.EMPTY_PARAMS, ex);
            builder.endObject();

            String expected = "{\"type\":\"file_not_found_exception\",\"reason\":\"foo not found\"}";
            assertEquals(expected, builder.string());
        }

        { // test equivalence
            ElasticsearchException ex =  new RemoteTransportException("foobar", new FileNotFoundException("foo not found"));
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ElasticsearchException.toXContent(builder, ToXContent.EMPTY_PARAMS, ex);
            builder.endObject();

            XContentBuilder otherBuilder = XContentFactory.jsonBuilder();

            otherBuilder.startObject();
            ex.toXContent(otherBuilder, ToXContent.EMPTY_PARAMS);
            otherBuilder.endObject();
            assertEquals(otherBuilder.string(), builder.string());
        }
    }

    public void testSerializeElasticsearchException() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        QueryParsingException ex = new QueryParsingException(new Index("foo"), "foobar");
        out.writeThrowable(ex);

        BytesStreamInput in = new BytesStreamInput(out.bytes());
        QueryParsingException e = in.readThrowable();
        assertEquals(ex.index(), e.index());
        assertEquals(ex.getMessage(), e.getMessage());
    }

}