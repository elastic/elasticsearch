/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.logging.AccumulatingMockAppender;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlExecutionInfoTests.createEsqlExecutionInfo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;

public class EsqlResponseListenerTests extends ESTestCase {
    private final String LOCAL_CLUSTER_ALIAS = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

    private static AccumulatingMockAppender appender;
    static Logger logger = LogManager.getLogger(EsqlResponseListener.class);

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new AccumulatingMockAppender("testAppender");
        appender.start();
        Configurator.setLevel(logger, Level.DEBUG);
        Loggers.addAppender(logger, appender);
    }

    @After
    public void clear() {
        appender.events.clear();
    }

    @AfterClass
    public static void cleanup() {
        appender.stop();
        Loggers.removeAppender(logger, appender);
    }

    public void testLogPartialFailures() {
        EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(false);
        executionInfo.swapCluster(
            LOCAL_CLUSTER_ALIAS,
            (k, v) -> new EsqlExecutionInfo.Cluster(
                LOCAL_CLUSTER_ALIAS,
                LOCAL_CLUSTER_ALIAS,
                "idx",
                false,
                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                10,
                10,
                3,
                0,
                List.of(
                    new ShardSearchFailure(new Exception("dummy"), target(LOCAL_CLUSTER_ALIAS, 0)),
                    new ShardSearchFailure(new Exception("error"), target(LOCAL_CLUSTER_ALIAS, 1))
                ),
                new TimeValue(4444L)
            )
        );
        EsqlResponseListener.logPartialFailures("/_query", Map.of(), executionInfo);

        assertThat(appender.events, hasSize(2));
        LogEvent logEvent = appender.events.get(0);
        assertThat(logEvent.getLevel(), equalTo(Level.WARN));
        assertThat(logEvent.getMessage().getFormattedMessage(), equalTo("partial failure at path: /_query, params: {}"));
        assertThat(logEvent.getThrown().getCause().getMessage(), equalTo("dummy"));
        logEvent = appender.events.get(1);
        assertThat(logEvent.getLevel(), equalTo(Level.WARN));
        assertThat(logEvent.getMessage().getFormattedMessage(), equalTo("partial failure at path: /_query, params: {}"));
        assertThat(logEvent.getThrown().getCause().getMessage(), equalTo("error"));
    }

    public void testLogPartialFailuresRemote() {
        EsqlExecutionInfo executionInfo = createEsqlExecutionInfo(false);
        executionInfo.swapCluster(
            "remote_cluster",
            (k, v) -> new EsqlExecutionInfo.Cluster(
                "remote_cluster",
                "remote_cluster",
                "idx",
                false,
                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                10,
                10,
                3,
                0,
                List.of(new ShardSearchFailure(new Exception("dummy"), target("remote_cluster", 0))),
                new TimeValue(4444L)
            )
        );
        EsqlResponseListener.logPartialFailures("/_query", Map.of(), executionInfo);

        assertThat(appender.events, hasSize(1));
        LogEvent logEvent = appender.events.get(0);
        assertThat(logEvent.getLevel(), equalTo(Level.WARN));
        assertThat(
            logEvent.getMessage().getFormattedMessage(),
            equalTo("partial failure at path: /_query, params: {}, cluster: remote_cluster")
        );
        assertThat(logEvent.getThrown().getCause().getMessage(), equalTo("dummy"));
    }

    public void testNdjsonFailureIsSentAsFooterLine() throws IOException {
        assumeTrue("format=ndjson is released with streaming, which is snapshot-only", RestEsqlQueryAction.STREAMING_ENABLED);
        RestRequest request = ndjsonRequest();
        FakeRestChannel channel = new FakeRestChannel(request, true);

        new EsqlResponseListener(channel, request, new EsqlQueryRequest(), new ThreadContext(Settings.EMPTY)).wrapWithLogging()
            .onFailure(new ElasticsearchStatusException("boom", RestStatus.BAD_REQUEST));

        RestResponse response = channel.capturedResponse();
        assertThat(response.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(response.contentType(), equalTo("application/x-ndjson"));
        List<Map<String, Object>> lines = parseNdjson(drain(response.chunkedContent()));
        assertThat("an error is a single line", lines, hasSize(1));
        assertThat(lines.get(0).get("status"), equalTo(400));
        @SuppressWarnings("unchecked")
        Map<String, Object> error = (Map<String, Object>) lines.get(0).get("error");
        assertThat(error.get("type"), equalTo("status_exception"));
        assertThat(error.get("reason"), equalTo(new ElasticsearchStatusException("boom", RestStatus.BAD_REQUEST).getDetailedMessage()));
    }

    public void testNdjsonSuccessCarriesWarningsAndTookHeader() throws IOException {
        assumeTrue("format=ndjson is released with streaming, which is snapshot-only", RestEsqlQueryAction.STREAMING_ENABLED);
        RestRequest request = ndjsonRequest();
        FakeRestChannel channel = new FakeRestChannel(request, true);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.addResponseHeader("Warning", HeaderWarning.formatWarning("No limit defined, adding default limit of [1000]"));
        EsqlQueryResponse queryResponse = new EsqlQueryResponse(
            List.of(new ColumnInfoImpl("a", "integer", null)),
            List.of(),
            0L,
            0L,
            null,
            false,
            false,
            ZoneOffset.UTC,
            0L,
            0L,
            createEsqlExecutionInfo(false)
        );

        new EsqlResponseListener(channel, request, new EsqlQueryRequest(), threadContext).wrapWithLogging().onResponse(queryResponse);
        queryResponse.decRef();

        RestResponse response = channel.capturedResponse();
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.contentType(), equalTo("application/x-ndjson"));
        assertThat(response.getHeaders(), hasKey("Took-nanos"));
        List<Map<String, Object>> lines = parseNdjson(drain(response.chunkedContent()));
        assertThat(lines, hasSize(2));
        assertThat(lines.get(1).get("warnings"), equalTo(List.of("No limit defined, adding default limit of [1000]")));
        response.close();
    }

    public void testNdjsonIsRejectedOnAsyncQueries() {
        assumeTrue("format=ndjson is released with streaming, which is snapshot-only", RestEsqlQueryAction.STREAMING_ENABLED);
        RestRequest request = ndjsonRequest();
        FakeRestChannel channel = new FakeRestChannel(request, true);

        IllegalArgumentException submit = expectThrows(
            IllegalArgumentException.class,
            () -> new EsqlResponseListener(
                channel,
                request,
                EsqlQueryRequest.asyncEsqlQueryRequest("ROW a = 1"),
                new ThreadContext(Settings.EMPTY)
            )
        );
        assertThat(submit.getMessage(), equalTo("[format=ndjson] is not supported on async queries"));

        IllegalArgumentException getResult = expectThrows(IllegalArgumentException.class, () -> new EsqlResponseListener(channel, request));
        assertThat(getResult.getMessage(), equalTo("[format=ndjson] is not supported on async queries"));
    }

    private static RestRequest ndjsonRequest() {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withHeaders(
            Map.of("Content-Type", Collections.singletonList("application/json"))
        ).withParams(Map.of("format", "ndjson")).build();
    }

    private static String drain(ChunkedRestResponseBodyPart part) throws IOException {
        List<BytesReference> chunks = new ArrayList<>();
        while (part.isPartComplete() == false) {
            chunks.add(part.encodeChunk(randomFrom(1, 64, 4096), BytesRefRecycler.NON_RECYCLING_INSTANCE));
        }
        assertTrue("the whole body is a single, final part", part.isLastPart());
        return CompositeBytesReference.of(chunks.toArray(new BytesReference[0])).utf8ToString();
    }

    private static List<Map<String, Object>> parseNdjson(String body) throws IOException {
        List<Map<String, Object>> lines = new ArrayList<>();
        for (String line : body.split("\n")) {
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, line)) {
                lines.add(parser.map());
            }
        }
        return lines;
    }

    private SearchShardTarget target(String clusterAlias, int shardId) {
        return new SearchShardTarget("node", new ShardId("idx", "uuid", shardId), clusterAlias);
    }
}
