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

import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.index.shard.IndexShardRecoveringException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.Collections;
import java.util.function.Supplier;

import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class ElasticsearchExceptionTests extends ESTestCase {

    public void testToXContent() throws IOException {
        ElasticsearchException e = new ElasticsearchException("test");
        assertToXContentAsJson(e, true, () -> equalTo("{\"type\":\"exception\",\"reason\":\"test\"}"));

        e = new IndexShardRecoveringException(new ShardId("_test", "_0", 5));
        assertToXContentAsJson(e, true, () -> equalTo("{\"type\":\"index_shard_recovering_exception\"," +
                "\"reason\":\"CurrentState[RECOVERING] Already recovering\",\"index_uuid\":\"_0\",\"shard\":\"5\",\"index\":\"_test\"}"));

        e = new BroadcastShardOperationFailedException(new ShardId("_index", "_uuid", 12), "foo", new IllegalStateException("bar"));
        assertToXContentAsJson(e, true, () -> equalTo("{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}"));

        e = new ElasticsearchException(new IllegalArgumentException("foo"));
        assertToXContentAsJson(e, true, () -> equalTo("{\"type\":\"exception\",\"reason\":\"java.lang.IllegalArgumentException: foo\"," +
                "\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"foo\"}}"));

        e = new ElasticsearchException("foo", new IllegalStateException("bar"));
        assertToXContentAsJson(e, true, () -> equalTo("{\"type\":\"exception\",\"reason\":\"foo\"," +
                "\"caused_by\":{\"type\":\"illegal_state_exception\",\"reason\":\"bar\"}}"));
        assertToXContentAsJson(e, false, () -> allOf(startsWith("{\"type\":\"exception\",\"reason\":\"foo\""),
                containsString("\"stack_trace\":\"java.lang.IllegalStateException: bar")));

    }

    public void testToXContentWithHeaders() throws IOException {
        ElasticsearchException e = new ElasticsearchException("foo",
                                        new ElasticsearchException("bar",
                                                new ElasticsearchException("baz",
                                                        new ClusterBlockException(singleton(DiscoverySettings.NO_MASTER_BLOCK_WRITES)))));
        e.addHeader("h0", "v0");
        e.addHeader("h1", "v1");
        e.addHeader("es.header_0", "es.v0");
        e.addHeader("es.header_1", "es.v1");

        final String expectedJson = "{"
            + "\"type\":\"exception\","
            + "\"reason\":\"foo\","
            + "\"header_0\":\"es.v0\","
            + "\"header_1\":\"es.v1\","
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
                    + "\"h0\":\"v0\","
                    + "\"h1\":\"v1\""
                + "}"
        + "}";

        assertToXContentAsJson(e, true, () -> equalTo(expectedJson));

        ElasticsearchException parsed;
        try (XContentParser parser = XContentType.JSON.xContent().createParser(expectedJson)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertThat(parsed.getMessage(), equalTo("Elasticsearch exception [type=exception, reason=foo]"));
        assertThat(parsed.getHeaderKeys(), hasSize(4));
        assertThat(parsed.getHeader("header_0").get(0), equalTo("es.v0"));
        assertThat(parsed.getHeader("header_1").get(0), equalTo("es.v1"));
        assertThat(parsed.getHeader("h0").get(0), equalTo("v0"));
        assertThat(parsed.getHeader("h1").get(0), equalTo("v1"));

        ElasticsearchException cause = (ElasticsearchException) parsed.getCause();
        assertThat(cause.getMessage(), equalTo("Elasticsearch exception [type=exception, reason=bar]"));

        cause = (ElasticsearchException) cause.getCause();
        assertThat(cause.getMessage(), equalTo("Elasticsearch exception [type=exception, reason=baz]"));

        cause = (ElasticsearchException) cause.getCause();
        assertThat(cause.getMessage(),
                equalTo("Elasticsearch exception [type=cluster_block_exception, reason=blocked by: [SERVICE_UNAVAILABLE/2/no master];]"));
    }

    public void testFromXContent() throws IOException {
        final XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent)
                                                    .startObject()
                                                        .field("type", "foo")
                                                        .field("reason", "something went wrong")
                                                    .endObject();

        ElasticsearchException parsed;
        try (XContentParser parser = xContent.createParser(builder.bytes())) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertThat(parsed.getMessage(), equalTo("Elasticsearch exception [type=foo, reason=something went wrong]"));
    }

    public void testFromXContentWithCause() throws IOException {
        ElasticsearchException e = new ElasticsearchException("foo",
                new ElasticsearchException("bar",
                        new ElasticsearchException("baz",
                                new RoutingMissingException("_test", "_type", "_id"))));

        final XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent).startObject().value(e).endObject();

        ElasticsearchException parsed;
        try (XContentParser parser = xContent.createParser(builder.bytes())) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = ElasticsearchException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertThat(parsed.getMessage(), equalTo("Elasticsearch exception [type=exception, reason=foo]"));

        ElasticsearchException cause = (ElasticsearchException) parsed.getCause();
        assertThat(cause.getMessage(), equalTo("Elasticsearch exception [type=exception, reason=bar]"));

        cause = (ElasticsearchException) cause.getCause();
        assertThat(cause.getMessage(), equalTo("Elasticsearch exception [type=exception, reason=baz]"));

        cause = (ElasticsearchException) cause.getCause();
        assertThat(cause.getMessage(),
                equalTo("Elasticsearch exception [type=routing_missing_exception, reason=routing is required for [_test]/[_type]/[_id]]"));
        assertThat(cause.getHeaderKeys(), hasSize(2));
        assertThat(cause.getHeader("index").size(), is(1));
        assertThat(cause.getHeader("index").get(0), equalTo("_test"));
        assertThat(cause.getHeader("index_uuid").size(), is(1));
        assertThat(cause.getHeader("index_uuid").get(0), equalTo("_na_"));
    }

    private static void assertToXContentAsJson(ToXContent toXContent, boolean skipTrace, Supplier<Matcher<String>> expected)
            throws IOException {
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        if (skipTrace == false) {
            params = new ToXContent.MapParams(Collections.singletonMap(ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
        }
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.startObject();
            toXContent.toXContent(builder, params);
            builder.endObject();
            assertThat(builder.bytes().utf8ToString(), expected.get());
        }
    }
}
