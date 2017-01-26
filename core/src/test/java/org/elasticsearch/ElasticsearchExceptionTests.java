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

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.shard.IndexShardRecoveringException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.elasticsearch.transport.RemoteTransportException;
import org.hamcrest.Matcher;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

public class ElasticsearchExceptionTests extends ESTestCase {

    public void testStatus() {
        ElasticsearchException exception = new ElasticsearchException("test");
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new ElasticsearchException("test", new RuntimeException());
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new ElasticsearchException("test", new ResourceNotFoundException("test"));
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        exception = new RemoteTransportException("test", new ResourceNotFoundException("test"));
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));

        exception = new RemoteTransportException("test", new ResourceAlreadyExistsException("test"));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));

        exception = new RemoteTransportException("test", new IllegalArgumentException("foobar"));
        assertThat(exception.status(), equalTo(RestStatus.BAD_REQUEST));

        exception = new RemoteTransportException("test", new IllegalStateException("foobar"));
        assertThat(exception.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testGuessRootCause() {
        {
            ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar",
                    new IndexNotFoundException("foo", new RuntimeException("foobar"))));
            ElasticsearchException[] rootCauses = exception.guessRootCauses();
            assertEquals(rootCauses.length, 1);
            assertEquals(ElasticsearchException.getExceptionName(rootCauses[0]), "index_not_found_exception");
            assertEquals(rootCauses[0].getMessage(), "no such index");
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
                    new ShardSearchFailure[]{failure, failure1});
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
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
                    new ShardSearchFailure[]{failure, failure1, failure2});
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
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
                    randomBoolean() ? failure1.getCause() : failure.getCause(), new ShardSearchFailure[]{failure, failure1});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\",\"phase\":\"search\"," +
                    "\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\",\"reason\":" +
                    "{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}}]}";
            assertEquals(expected, builder.string());
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo1", "_na_"), 1));
            ShardSearchFailure failure2 = new ShardSearchFailure(new QueryShardException(new Index("foo1", "_na_"), "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo1", "_na_"), 2));
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed",
                    new ShardSearchFailure[]{failure, failure1, failure2});
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\"," +
                    "\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\"," +
                    "\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}},{\"shard\":1," +
                    "\"index\":\"foo1\",\"node\":\"node_1\",\"reason\":{\"type\":\"query_shard_exception\",\"reason\":\"foobar\"," +
                    "\"index_uuid\":\"_na_\",\"index\":\"foo1\"}}]}";
            assertEquals(expected, builder.string());
        }
        {
            ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 1));
            ShardSearchFailure failure1 = new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new Index("foo", "_na_"), 2));
            NullPointerException nullPointerException = new NullPointerException();
            SearchPhaseExecutionException ex = new SearchPhaseExecutionException("search", "all shards failed", nullPointerException,
                    new ShardSearchFailure[]{failure, failure1});
            assertEquals(nullPointerException, ex.getCause());
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            ex.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String expected = "{\"type\":\"search_phase_execution_exception\",\"reason\":\"all shards failed\"," +
                    "\"phase\":\"search\",\"grouped\":true,\"failed_shards\":[{\"shard\":1,\"index\":\"foo\",\"node\":\"node_1\"," +
                    "\"reason\":{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}}]," +
                    "\"caused_by\":{\"type\":\"null_pointer_exception\",\"reason\":null}}";
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
    private static boolean contains(Throwable t, Class<? extends Throwable> exType) {
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
        ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar",
                new IllegalArgumentException("index is closed", root)));
        assertEquals(root, exception.getRootCause());
        assertTrue(contains(exception, RuntimeException.class));
        assertFalse(contains(exception, EOFException.class));
    }

    public void testToString() {
        ElasticsearchException exception = new ElasticsearchException("foo", new ElasticsearchException("bar",
                new IllegalArgumentException("index is closed", new RuntimeException("foobar"))));
        assertEquals("ElasticsearchException[foo]; nested: ElasticsearchException[bar]; nested: IllegalArgumentException" +
                "[index is closed]; nested: RuntimeException[foobar];", exception.toString());
    }

    public void testToXContent() throws IOException {
        {
            ElasticsearchException e = new ElasticsearchException("test");
            assertExceptionAsJson(e, "{\"type\":\"exception\",\"reason\":\"test\"}");
        }
        {
            ElasticsearchException e = new IndexShardRecoveringException(new ShardId("_test", "_0", 5));
            assertExceptionAsJson(e, "{\"type\":\"index_shard_recovering_exception\"," +
                    "\"reason\":\"CurrentState[RECOVERING] Already recovering\",\"index_uuid\":\"_0\"," +
                    "\"shard\":\"5\",\"index\":\"_test\"}");
        }
        {
            ElasticsearchException e = new BroadcastShardOperationFailedException(new ShardId("_index", "_uuid", 12), "foo",
                    new IllegalStateException("bar"));
            assertExceptionAsJson(e, "{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}");
        }
        {
            ElasticsearchException e = new ElasticsearchException(new IllegalArgumentException("foo"));
            assertExceptionAsJson(e, "{\"type\":\"exception\",\"reason\":\"java.lang.IllegalArgumentException: foo\"," +
                    "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"foo\"}}");
        }
        {
            ElasticsearchException e = new SearchParseException(new TestSearchContext(null), "foo", new XContentLocation(1,0));
            assertExceptionAsJson(e, "{\"type\":\"search_parse_exception\",\"reason\":\"foo\",\"line\":1,\"col\":0}");
        }
        {
            ElasticsearchException ex = new ElasticsearchException("foo",
                    new ElasticsearchException("bar", new IllegalArgumentException("index is closed", new RuntimeException("foobar"))));
            assertExceptionAsJson(ex, "{\"type\":\"exception\",\"reason\":\"foo\",\"caused_by\":{\"type\":\"exception\"," +
                    "\"reason\":\"bar\",\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"index is closed\"," +
                    "\"caused_by\":{\"type\":\"runtime_exception\",\"reason\":\"foobar\"}}}}");
        }
        {
            ElasticsearchException e = new ElasticsearchException("foo", new IllegalStateException("bar"));
            assertExceptionAsJson(e, "{\"type\":\"exception\",\"reason\":\"foo\"," +
                    "\"caused_by\":{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}}");

            // Test the same exception but with the "rest.exception.stacktrace.skip" parameter disabled: the stack_trace must be present
            // in the JSON. Since the stack can be large, it only checks the beginning of the JSON.
            ToXContent.Params params = new ToXContent.MapParams(
                    Collections.singletonMap(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
            String actual;
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                builder.startObject();
                e.toXContent(builder, params);
                builder.endObject();
                actual = builder.string();
            }
            assertThat(actual, startsWith("{\"type\":\"exception\",\"reason\":\"foo\"," +
                    "\"caused_by\":{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"," +
                    "\"stack_trace\":\"java.lang.IllegalStateException: bar" +
                    (Constants.WINDOWS ? "\\r\\n" : "\\n") +
                    "\\tat org.elasticsearch."));
        }
    }

    public void testGenerateThrowableToXContent() throws IOException {
        {
            Exception ex;
            if (randomBoolean()) {
                // just a wrapper which is omitted
                ex = new RemoteTransportException("foobar", new FileNotFoundException("foo not found"));
            } else {
                ex = new FileNotFoundException("foo not found");
            }
            assertExceptionAsJson(ex, "{\"type\":\"file_not_found_exception\",\"reason\":\"foo not found\"}");
        }
        {
            ParsingException ex = new ParsingException(1, 2, "foobar", null);
            assertExceptionAsJson(ex, "{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2}");
        }

        { // test equivalence
            ElasticsearchException ex =  new RemoteTransportException("foobar", new FileNotFoundException("foo not found"));
            String toXContentString = Strings.toString(ex);
            String throwableString = Strings.toString((builder, params) -> {
                ElasticsearchException.generateThrowableXContent(builder, params, ex);
                return builder;
            });

            assertEquals(throwableString, toXContentString);
            assertEquals("{\"type\":\"file_not_found_exception\",\"reason\":\"foo not found\"}", toXContentString);
        }

        { // render header and metadata
            ParsingException ex = new ParsingException(1, 2, "foobar", null);
            ex.addMetadata("es.test1", "value1");
            ex.addMetadata("es.test2", "value2");
            ex.addHeader("test", "some value");
            ex.addHeader("test_multi", "some value", "another value");
            String expected = "{\"type\":\"parsing_exception\",\"reason\":\"foobar\",\"line\":1,\"col\":2," +
                    "\"test1\":\"value1\",\"test2\":\"value2\"," +
                    "\"header\":{\"test_multi\":" +
                    "[\"some value\",\"another value\"],\"test\":\"some value\"}}";
            assertExceptionAsJson(ex, expected);
        }
    }

    public void testToXContentWithHeadersAndMetadata() throws IOException {
        ElasticsearchException e = new ElasticsearchException("foo",
                                        new ElasticsearchException("bar",
                                                new ElasticsearchException("baz",
                                                        new ClusterBlockException(singleton(DiscoverySettings.NO_MASTER_BLOCK_WRITES)))));
        e.addHeader("foo_0", "0");
        e.addHeader("foo_1", "1");
        e.addMetadata("es.metadata_foo_0", "foo_0");
        e.addMetadata("es.metadata_foo_1", "foo_1");

        final String expectedJson = "{"
            + "\"type\":\"exception\","
            + "\"reason\":\"foo\","
            + "\"metadata_foo_0\":\"foo_0\","
            + "\"metadata_foo_1\":\"foo_1\","
            + "\"caused_by\":{"
                + "\"type\":\"exception\","
                + "\"reason\":\"bar\","
                + "\"caused_by\":{"
                    + "\"type\":\"exception\","
                    + "\"reason\":\"baz\","
                    + "\"caused_by\":{"
                        + "\"type\":\"cluster_block_exception\","
                        + "\"reason\":\"blocked by: [SERVICE_UNAVAILABLE/2/no master];\""
                    + "}"
                + "}"
            + "},"
            + "\"header\":{"
                    + "\"foo_0\":\"0\","
                    + "\"foo_1\":\"1\""
                + "}"
        + "}";

        assertExceptionAsJson(e, expectedJson);

        ElasticsearchException parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), expectedJson)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Elasticsearch exception [type=exception, reason=foo]");
        assertThat(parsed.getHeaderKeys(), hasSize(2));
        assertEquals(parsed.getHeader("foo_0").get(0), "0");
        assertEquals(parsed.getHeader("foo_1").get(0), "1");
        assertThat(parsed.getMetadataKeys(), hasSize(2));
        assertEquals(parsed.getMetadata("es.metadata_foo_0").get(0), "foo_0");
        assertEquals(parsed.getMetadata("es.metadata_foo_1").get(0), "foo_1");

        ElasticsearchException cause = (ElasticsearchException) parsed.getCause();
        assertEquals(cause.getMessage(), "Elasticsearch exception [type=exception, reason=bar]");

        cause = (ElasticsearchException) cause.getCause();
        assertEquals(cause.getMessage(), "Elasticsearch exception [type=exception, reason=baz]");

        cause = (ElasticsearchException) cause.getCause();
        assertEquals(cause.getMessage(),
                "Elasticsearch exception [type=cluster_block_exception, reason=blocked by: [SERVICE_UNAVAILABLE/2/no master];]");
    }

    public void testFromXContent() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent)
                                                    .startObject()
                                                        .field("type", "foo")
                                                        .field("reason", "something went wrong")
                                                        .field("stack_trace", "...")
                                                    .endObject();

        ElasticsearchException parsed;
        try (XContentParser parser = createParser(xContent, builder.bytes())) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Elasticsearch exception [type=foo, reason=something went wrong, stack_trace=...]");
    }

    public void testFromXContentWithCause() throws IOException {
        ElasticsearchException e = new ElasticsearchException("foo",
                new ElasticsearchException("bar",
                        new ElasticsearchException("baz",
                                new RoutingMissingException("_test", "_type", "_id"))));

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent).startObject().value(e).endObject();

        ElasticsearchException parsed;
        try (XContentParser parser = createParser(builder)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Elasticsearch exception [type=exception, reason=foo]");

        ElasticsearchException cause = (ElasticsearchException) parsed.getCause();
        assertEquals(cause.getMessage(), "Elasticsearch exception [type=exception, reason=bar]");

        cause = (ElasticsearchException) cause.getCause();
        assertEquals(cause.getMessage(), "Elasticsearch exception [type=exception, reason=baz]");

        cause = (ElasticsearchException) cause.getCause();
        assertEquals(cause.getMessage(),
                "Elasticsearch exception [type=routing_missing_exception, reason=routing is required for [_test]/[_type]/[_id]]");
        assertThat(cause.getHeaderKeys(), hasSize(0));
        assertThat(cause.getMetadataKeys(), hasSize(2));
        assertThat(cause.getMetadata("es.index"), hasItem("_test"));
        assertThat(cause.getMetadata("es.index_uuid"), hasItem("_na_"));
    }

    public void testFromXContentWithHeadersAndMetadata() throws IOException {
        RoutingMissingException routing = new RoutingMissingException("_test", "_type", "_id");
        ElasticsearchException baz = new ElasticsearchException("baz", routing);
        baz.addHeader("baz_0", "baz0");
        baz.addMetadata("es.baz_1", "baz1");
        baz.addHeader("baz_2", "baz2");
        baz.addMetadata("es.baz_3", "baz3");
        ElasticsearchException bar = new ElasticsearchException("bar", baz);
        bar.addMetadata("es.bar_0", "bar0");
        bar.addHeader("bar_1", "bar1");
        bar.addMetadata("es.bar_2", "bar2");
        ElasticsearchException foo = new ElasticsearchException("foo", bar);
        foo.addMetadata("es.foo_0", "foo0");
        foo.addHeader("foo_1", "foo1");

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent).startObject().value(foo).endObject();

        ElasticsearchException parsed;
        try (XContentParser parser = createParser(builder)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Elasticsearch exception [type=exception, reason=foo]");
        assertThat(parsed.getHeaderKeys(), hasSize(1));
        assertThat(parsed.getHeader("foo_1"), hasItem("foo1"));
        assertThat(parsed.getMetadataKeys(), hasSize(1));
        assertThat(parsed.getMetadata("es.foo_0"), hasItem("foo0"));

        ElasticsearchException cause = (ElasticsearchException) parsed.getCause();
        assertEquals(cause.getMessage(), "Elasticsearch exception [type=exception, reason=bar]");
        assertThat(cause.getHeaderKeys(), hasSize(1));
        assertThat(cause.getHeader("bar_1"), hasItem("bar1"));
        assertThat(cause.getMetadataKeys(), hasSize(2));
        assertThat(cause.getMetadata("es.bar_0"), hasItem("bar0"));
        assertThat(cause.getMetadata("es.bar_2"), hasItem("bar2"));

        cause = (ElasticsearchException) cause.getCause();
        assertEquals(cause.getMessage(), "Elasticsearch exception [type=exception, reason=baz]");
        assertThat(cause.getHeaderKeys(), hasSize(2));
        assertThat(cause.getHeader("baz_0"), hasItem("baz0"));
        assertThat(cause.getHeader("baz_2"), hasItem("baz2"));
        assertThat(cause.getMetadataKeys(), hasSize(2));
        assertThat(cause.getMetadata("es.baz_1"), hasItem("baz1"));
        assertThat(cause.getMetadata("es.baz_3"), hasItem("baz3"));

        cause = (ElasticsearchException) cause.getCause();
        assertEquals(cause.getMessage(),
                "Elasticsearch exception [type=routing_missing_exception, reason=routing is required for [_test]/[_type]/[_id]]");
        assertThat(cause.getHeaderKeys(), hasSize(0));
        assertThat(cause.getMetadataKeys(), hasSize(2));
        assertThat(cause.getMetadata("es.index"), hasItem("_test"));
        assertThat(cause.getMetadata("es.index_uuid"), hasItem("_na_"));
    }

    public void testThrowableToAndFromXContent() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();

        final Tuple<Throwable, ElasticsearchException> exceptions = randomExceptions();
        final Throwable throwable = exceptions.v1();

        BytesReference throwableBytes = XContentHelper.toXContent((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, params, throwable);
            return builder;
        }, xContent.type(), randomBoolean());

        ElasticsearchException parsedException;
        try (XContentParser parser = createParser(xContent, throwableBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedException = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
        assertNotNull(parsedException);

        ElasticsearchException expected = exceptions.v2();
        do {
            assertEquals(expected.getMessage(), parsedException.getMessage());
            assertEquals(expected.getHeaders(), parsedException.getHeaders());
            assertEquals(expected.getMetadata(), parsedException.getMetadata());
            assertEquals(expected.getResourceType(), parsedException.getResourceType());
            assertEquals(expected.getResourceId(), parsedException.getResourceId());

            expected = (ElasticsearchException) expected.getCause();
            parsedException = (ElasticsearchException) parsedException.getCause();
            if (expected == null) {
                assertNull(parsedException);
            }
        } while (expected != null);
    }

    /**
     * Builds a {@link ToXContent} using a JSON XContentBuilder and check the resulting string with the given {@link Matcher}.
     *
     * By default, the stack trace of the exception is not rendered. The parameter `errorTrace` forces the stack trace to
     * be rendered like the REST API does when the "error_trace" parameter is set to true.
     */
    private static void assertToXContentAsJson(ToXContent e, String expectedJson) throws IOException {
        BytesReference actual = XContentHelper.toXContent(e, XContentType.JSON, randomBoolean());
        assertToXContentEquivalent(new BytesArray(expectedJson), actual, XContentType.JSON);
    }

    private static void assertExceptionAsJson(Exception e, String expectedJson) throws IOException {
        assertToXContentAsJson((builder, params) -> {
            ElasticsearchException.generateThrowableXContent(builder, params, e);
            return builder;
        }, expectedJson);
    }

    private static Tuple<Throwable, ElasticsearchException> randomExceptions() {
        Throwable actual;
        ElasticsearchException expected;

        int type = randomIntBetween(0, 5);
        switch (type) {
            case 0:
                actual = new ClusterBlockException(singleton(DiscoverySettings.NO_MASTER_BLOCK_WRITES));
                expected = new ElasticsearchException("Elasticsearch exception [type=cluster_block_exception, " +
                        "reason=blocked by: [SERVICE_UNAVAILABLE/2/no master];]");
                break;
            case 1:
                actual = new CircuitBreakingException("Data too large", 123, 456);
                expected = new ElasticsearchException("Elasticsearch exception [type=circuit_breaking_exception, reason=Data too large]");
                expected.addMetadata("es.bytes_wanted", "123");
                expected.addMetadata("es.bytes_limit", "456");
                break;
            case 2:
                actual = new SearchParseException(new TestSearchContext(null), "Parse failure", new XContentLocation(12, 98));
                expected = new ElasticsearchException("Elasticsearch exception [type=search_parse_exception, reason=Parse failure]");
                expected.addMetadata("es.line", "12");
                expected.addMetadata("es.col", "98");
                break;
            case 3:
                actual = new IllegalArgumentException("Closed resource", new RuntimeException("Resource"));
                expected = new ElasticsearchException("Elasticsearch exception [type=illegal_argument_exception, reason=Closed resource]",
                                new ElasticsearchException("Elasticsearch exception [type=runtime_exception, reason=Resource]"));
                break;
            case 4:
                actual = new SearchPhaseExecutionException("search", "all shards failed",
                            new ShardSearchFailure[]{
                                    new ShardSearchFailure(new ParsingException(1, 2, "foobar", null),
                                            new SearchShardTarget("node_1", new Index("foo", "_na_"), 1))
                            });
                expected = new ElasticsearchException("Elasticsearch exception [type=search_phase_execution_exception, " +
                        "reason=all shards failed]");
                expected.addMetadata("es.grouped", "true");
                expected.addMetadata("es.phase", "search");
                break;
            case 5:
                actual = new ElasticsearchException("Parsing failed",
                            new ParsingException(9, 42, "Wrong state",
                                new NullPointerException("Unexpected null value")));

                ElasticsearchException expectedCause = new ElasticsearchException("Elasticsearch exception [type=parsing_exception, " +
                        "reason=Wrong state]", new ElasticsearchException("Elasticsearch exception [type=null_pointer_exception, " +
                        "reason=Unexpected null value]"));
                expectedCause.addMetadata("es.line", "9");
                expectedCause.addMetadata("es.col", "42");

                expected = new ElasticsearchException("Elasticsearch exception [type=exception, reason=Parsing failed]", expectedCause);
                break;
            default:
                throw new UnsupportedOperationException("No randomized exceptions generated for type [" + type + "]");
        }

        if (actual instanceof ElasticsearchException) {
            ElasticsearchException actualException = (ElasticsearchException) actual;
            if (randomBoolean()) {
                int nbHeaders = randomIntBetween(1, 5);
                Map<String, List<String>> randomHeaders = new HashMap<>(nbHeaders);

                for (int i = 0; i < nbHeaders; i++) {
                    List<String> values = new ArrayList<>();

                    int nbValues = randomIntBetween(1, 3);
                    for (int j = 0; j < nbValues; j++) {
                        values.add(frequently() ? randomAsciiOfLength(5) : null);
                    }
                    randomHeaders.put("header_" + i, values);
                }

                for (Map.Entry<String, List<String>> entry : randomHeaders.entrySet()) {
                    actualException.addHeader(entry.getKey(), entry.getValue());
                    expected.addHeader(entry.getKey(), entry.getValue());
                }
            }

            if (randomBoolean()) {
                int nbMetadata = randomIntBetween(1, 5);
                Map<String, List<String>> randomMetadata = new HashMap<>(nbMetadata);

                for (int i = 0; i < nbMetadata; i++) {
                    List<String> values = new ArrayList<>();

                    int nbValues = randomIntBetween(1, 3);
                    for (int j = 0; j < nbValues; j++) {
                        values.add(frequently() ? randomAsciiOfLength(5) : null);
                    }
                    randomMetadata.put("es.metadata_" + i, values);
                }

                for (Map.Entry<String, List<String>> entry : randomMetadata.entrySet()) {
                    actualException.addMetadata(entry.getKey(), entry.getValue());
                    expected.addMetadata(entry.getKey(), entry.getValue());
                }
            }

            if (randomBoolean()) {
                int nbResources = randomIntBetween(1, 5);
                for (int i = 0; i < nbResources; i++) {
                    String resourceType = "type_" + i;
                    String[] resourceIds = null;
                    if (frequently()) {
                        resourceIds = new String[randomIntBetween(1, 3)];
                        for (int j = 0; j < resourceIds.length; j++) {
                            resourceIds[j] = frequently() ? randomAsciiOfLength(5) : null;
                        }
                    }
                    actualException.setResources(resourceType, resourceIds);
                    expected.setResources(resourceType, resourceIds);
                }
            }
        }
        return new Tuple<>(actual, expected);
    }
}
