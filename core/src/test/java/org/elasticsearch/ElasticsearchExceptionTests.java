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
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.shard.IndexShardRecoveringException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

public class ElasticsearchExceptionTests extends ESTestCase {

    public void testToXContent() throws IOException {
        ElasticsearchException e = new ElasticsearchException("test");
        assertExceptionAsJson(e, false, equalTo("{\"type\":\"exception\",\"reason\":\"test\"}"));

        e = new IndexShardRecoveringException(new ShardId("_test", "_0", 5));
        assertExceptionAsJson(e, false, equalTo("{\"type\":\"index_shard_recovering_exception\"," +
                "\"reason\":\"CurrentState[RECOVERING] Already recovering\",\"index_uuid\":\"_0\",\"shard\":\"5\",\"index\":\"_test\"}"));

        e = new BroadcastShardOperationFailedException(new ShardId("_index", "_uuid", 12), "foo", new IllegalStateException("bar"));
        assertExceptionAsJson(e, false, equalTo("{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}"));

        e = new ElasticsearchException(new IllegalArgumentException("foo"));
        assertExceptionAsJson(e, false, equalTo("{\"type\":\"exception\",\"reason\":\"java.lang.IllegalArgumentException: foo\"," +
                "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"foo\"}}"));

        e = new ElasticsearchException("foo", new IllegalStateException("bar"));
        assertExceptionAsJson(e, false, equalTo("{\"type\":\"exception\",\"reason\":\"foo\"," +
                "\"caused_by\":{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}}"));

        // Test the same exception but with the "rest.exception.stacktrace.skip" parameter disabled: the stack_trace must be present
        // in the JSON. Since the stack can be large, it only checks the beginning of the JSON.
        assertExceptionAsJson(e, true, startsWith("{\"type\":\"exception\",\"reason\":\"foo\"," +
                "\"caused_by\":{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"," +
                "\"stack_trace\":\"java.lang.IllegalStateException: bar" +
                (Constants.WINDOWS ? "\\r\\n" : "\\n") +
                "\\tat org.elasticsearch."));
    }

    public void testToXContentWithHeaders() throws IOException {
        ElasticsearchException e = new ElasticsearchException("foo",
                                        new ElasticsearchException("bar",
                                                new ElasticsearchException("baz",
                                                        new ClusterBlockException(singleton(DiscoverySettings.NO_MASTER_BLOCK_WRITES)))));
        e.addHeader("bar_0", "0");
        e.addHeader("bar_1", "1");
        e.addHeader("es.foo_0", "foo_0");
        e.addHeader("es.foo_1", "foo_1");

        final String expectedJson = "{"
            + "\"type\":\"exception\","
            + "\"reason\":\"foo\","
            + "\"foo_0\":\"foo_0\","
            + "\"foo_1\":\"foo_1\","
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
                    + "\"bar_0\":\"0\","
                    + "\"bar_1\":\"1\""
                + "}"
        + "}";

        assertExceptionAsJson(e, false, equalTo(expectedJson));

        ElasticsearchException parsed;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), expectedJson)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.getMessage(), "Elasticsearch exception [type=exception, reason=foo]");
        assertThat(parsed.getHeaderKeys(), hasSize(4));
        assertEquals(parsed.getHeader("foo_0").get(0), "foo_0");
        assertEquals(parsed.getHeader("foo_1").get(0), "foo_1");
        assertEquals(parsed.getHeader("header.bar_0").get(0), "0");
        assertEquals(parsed.getHeader("header.bar_1").get(0), "1");

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
        assertThat(cause.getHeaderKeys(), hasSize(2));
        assertThat(cause.getHeader("index"), hasItem("_test"));
        assertThat(cause.getHeader("index_uuid"), hasItem("_na_"));
    }

    public void testFromXContentWithHeaders() throws IOException {
        RoutingMissingException routing = new RoutingMissingException("_test", "_type", "_id");
        ElasticsearchException baz = new ElasticsearchException("baz", routing);
        baz.addHeader("baz_0", "baz0");
        baz.addHeader("es.baz_1", "baz1");
        baz.addHeader("baz_2", "baz2");
        baz.addHeader("es.baz_3", "baz3");
        ElasticsearchException bar = new ElasticsearchException("bar", baz);
        bar.addHeader("es.bar_0", "bar0");
        bar.addHeader("bar_1", "bar1");
        bar.addHeader("es.bar_2", "bar2");
        ElasticsearchException foo = new ElasticsearchException("foo", bar);
        foo.addHeader("es.foo_0", "foo0");
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
        assertThat(parsed.getHeaderKeys(), hasSize(2));
        assertThat(parsed.getHeader("foo_0"), hasItem("foo0"));
        assertThat(parsed.getHeader("header.foo_1"), hasItem("foo1"));

        ElasticsearchException cause = (ElasticsearchException) parsed.getCause();
        assertEquals(cause.getMessage(), "Elasticsearch exception [type=exception, reason=bar]");
        assertThat(cause.getHeaderKeys(), hasSize(3));
        assertThat(cause.getHeader("bar_0"), hasItem("bar0"));
        assertThat(cause.getHeader("header.bar_1"), hasItem("bar1"));
        assertThat(cause.getHeader("bar_2"), hasItem("bar2"));

        cause = (ElasticsearchException) cause.getCause();
        assertEquals(cause.getMessage(), "Elasticsearch exception [type=exception, reason=baz]");
        assertThat(cause.getHeaderKeys(), hasSize(4));
        assertThat(cause.getHeader("header.baz_0"), hasItem("baz0"));
        assertThat(cause.getHeader("baz_1"), hasItem("baz1"));
        assertThat(cause.getHeader("header.baz_2"), hasItem("baz2"));
        assertThat(cause.getHeader("baz_3"), hasItem("baz3"));

        cause = (ElasticsearchException) cause.getCause();
        assertEquals(cause.getMessage(),
                "Elasticsearch exception [type=routing_missing_exception, reason=routing is required for [_test]/[_type]/[_id]]");
        assertThat(cause.getHeaderKeys(), hasSize(2));
        assertThat(cause.getHeader("index"), hasItem("_test"));
        assertThat(cause.getHeader("index_uuid"), hasItem("_na_"));
    }

    public void testFromXContentWithExtraMetadataFields() throws IOException {
        CircuitBreakingException exception = new CircuitBreakingException("test", randomNonNegativeLong(), randomNonNegativeLong());

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        BytesReference exceptionBytes = XContentHelper.toXContent(exception, xContent.type());

        ElasticsearchException parsedException;
        try (XContentParser parser = createParser(xContent, exceptionBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedException = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertThat(parsedException.getHeaderKeys(), hasSize(2));
        assertThat(parsedException.getHeader("bytes_wanted"), hasItem(String.valueOf(exception.getBytesWanted())));
        assertThat(parsedException.getHeader("bytes_limit"), hasItem(String.valueOf(exception.getByteLimit())));
    }

    public void testFromXContentWithExtraMetadataArrays() throws IOException {
        Index index = new Index("test", "_na");
        SearchShardTarget searchShardTarget = new SearchShardTarget("foo", index, 3);
        QueryShardException queryShardException = new QueryShardException(index, "bar", new IllegalArgumentException("baz"));
        ShardSearchFailure failure1 = new ShardSearchFailure(queryShardException, searchShardTarget);
        ShardSearchFailure failure2 = new ShardSearchFailure(new NullPointerException("boo"), searchShardTarget);
        ShardSearchFailure[] failures = new ShardSearchFailure[]{failure1, failure2};
        SearchPhaseExecutionException exception = new SearchPhaseExecutionException("qux", "test", failures);

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        BytesReference exceptionBytes = XContentHelper.toXContent(exception, xContent.type());

        ElasticsearchException parsedException;
        try (XContentParser parser = createParser(xContent, exceptionBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedException = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        Map<String, List<String>> parsedHeaders = parsedException.getHeaders();
        assertEquals(16, parsedHeaders.size());

        assertThat(parsedHeaders, allOf(
                hasEntry("phase", singletonList("qux")),
                hasEntry("grouped", singletonList("true")),

                hasEntry("failed_shards.0.node", singletonList("foo")),
                hasEntry("failed_shards.0.shard", singletonList("3")),
                hasEntry("failed_shards.0.index", singletonList("test")),
                hasEntry("failed_shards.0.reason.type", singletonList("query_shard_exception")),
                hasEntry("failed_shards.0.reason.reason", singletonList("bar")),
                hasEntry("failed_shards.0.reason.index", singletonList("test")),
                hasEntry("failed_shards.0.reason.index_uuid", singletonList("_na")),
                hasEntry("failed_shards.0.reason.caused_by.type", singletonList("illegal_argument_exception")),
                hasEntry("failed_shards.0.reason.caused_by.reason", singletonList("baz")),

                hasEntry("failed_shards.1.node", singletonList("foo")),
                hasEntry("failed_shards.1.shard", singletonList("3")),
                hasEntry("failed_shards.1.index", singletonList("test")),
                hasEntry("failed_shards.1.reason.type", singletonList("null_pointer_exception")),
                hasEntry("failed_shards.1.reason.reason", singletonList("boo"))
        ));
    }

    public void testFromXContentWithExtraMetadataObjects() throws IOException {
        String json = "{\"type\":\"exception_with_objects\",\"reason\":\"test\"," +
                "\"a\":{\"b\":true,\"c\":{\"d\":0}},\"e\":\"f\", \"g\":[\"h\",\"i\",\"j\",\"k\"]}";

        ElasticsearchException parsedException;
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), new BytesArray(json))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedException = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        Map<String, List<String>> parsedHeaders = parsedException.getHeaders();
        assertEquals(4, parsedHeaders.size());

        assertThat(parsedHeaders, allOf(
                hasEntry("a.b", singletonList("true")),
                hasEntry("a.c.d", singletonList("0")),
                hasEntry("e", singletonList("f")),
                hasEntry("g", Arrays.asList("h", "i", "j", "k"))
        ));
    }

    /**
     * Builds a {@link ToXContent} using a JSON XContentBuilder and check the resulting string with the given {@link Matcher}.
     *
     * By default, the stack trace of the exception is not rendered. The parameter `errorTrace` forces the stack trace to
     * be rendered like the REST API does when the "error_trace" parameter is set to true.
     */
    private static void assertExceptionAsJson(ElasticsearchException e, boolean errorTrace, Matcher<String> expected)
            throws IOException {
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        if (errorTrace) {
            params = new ToXContent.MapParams(Collections.singletonMap(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
        }
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            e.toXContent(builder, params);
            builder.endObject();
            assertThat(builder.bytes().utf8ToString(), expected);
        }
    }
}
