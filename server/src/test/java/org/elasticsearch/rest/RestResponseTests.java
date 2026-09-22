/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import co.elastic.logging.log4j2.EcsLayout;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.message.MapMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.logging.MockAppender;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;
import static org.elasticsearch.ElasticsearchExceptionTests.assertDeepEquals;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.rest.RestController.ERROR_TRACE_DEFAULT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class RestResponseTests extends ESTestCase {

    private static MockAppender appender;
    static Logger restSuppressedLogger = LogManager.getLogger("rest.suppressed");

    @BeforeClass
    public static void init() throws IllegalAccessException {
        appender = new MockAppender("testAppender");
        appender.start();
        Configurator.setLevel(restSuppressedLogger, Level.DEBUG);
        Loggers.addAppender(restSuppressedLogger, appender);
    }

    @AfterClass
    public static void cleanup() {
        appender.stop();
        Loggers.removeAppender(restSuppressedLogger, appender);
    }

    class UnknownException extends Exception {
        UnknownException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }

    public void testWithHeaders() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = randomBoolean() ? new DetailedExceptionRestChannel(request) : new SimpleExceptionRestChannel(request);

        RestResponse response = new RestResponse(channel, new WithHeadersException());
        assertEquals(3, response.getHeaders().size());
        assertThat(response.getHeaders().get("n1"), notNullValue());
        assertThat(response.getHeaders().get("n1"), contains("v11", "v12"));
        assertThat(response.getHeaders().get("n2"), notNullValue());
        assertThat(response.getHeaders().get("n2"), contains("v21", "v22"));
        assertThat(response.getHeaders().get("My-Header"), notNullValue());
        assertThat(response.getHeaders().get("My-Header"), contains("v1"));
    }

    public void testEmptyChunkedBody() {
        RestResponse response = RestResponse.chunked(
            RestStatus.OK,
            ChunkedRestResponseBodyPart.fromTextChunks(RestResponse.TEXT_CONTENT_TYPE, Collections.emptyIterator()),
            null
        );
        assertFalse(response.isChunked());
        assertNotNull(response.content());
        assertEquals(0, response.content().length());
    }

    public void testSimpleExceptionMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        Exception t = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        RestResponse response = new RestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("""
            {"type":"exception","reason":"an error occurred reading data"}"""));
        assertThat(text, not(containsString("file_not_found_exception")));
        assertThat(text, not(containsString("/foo/bar")));
        assertThat(text, not(containsString("error_trace")));
    }

    public void testDetailedExceptionMessage() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);

        Exception t = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        RestResponse response = new RestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("""
            {"type":"exception","reason":"an error occurred reading data"}"""));
        assertThat(text, containsString("""
            {"type":"file_not_found_exception","reason":"/foo/bar"}"""));
    }

    public void testErrorTrace() throws Exception {
        RestRequest request = new FakeRestRequest();
        request.params().put("error_trace", "true");
        RestChannel channel = new DetailedExceptionRestChannel(request);

        Exception t = new UnknownException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        RestResponse response = new RestResponse(channel, t);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("""
            "type":"unknown_exception","reason":"an error occurred reading data\""""));
        assertThat(text, containsString("""
            {"type":"file_not_found_exception\""""));
        assertThat(text, containsString("""
            "stack_trace":"org.elasticsearch.ElasticsearchException$1: an error occurred reading data"""));
    }

    public void testAuthenticationFailedNoStackTrace() throws IOException {
        for (Exception authnException : List.of(
            new ElasticsearchSecurityException("failed authn", RestStatus.UNAUTHORIZED),
            new ElasticsearchSecurityException("failed authn", RestStatus.UNAUTHORIZED, new ElasticsearchException("cause"))
        )) {
            for (RestRequest request : List.of(
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build(),
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of("error_trace", Boolean.toString(true))).build()
            )) {
                for (RestChannel channel : List.of(new SimpleExceptionRestChannel(request), new DetailedExceptionRestChannel(request))) {
                    RestResponse response = new RestResponse(channel, authnException);
                    assertThat(response.status(), is(RestStatus.UNAUTHORIZED));
                    assertThat(response.content().utf8ToString(), not(containsString(ElasticsearchException.STACK_TRACE)));
                }
            }
        }
    }

    public void testStackTrace() throws IOException {
        for (Exception exception : List.of(new ElasticsearchException("dummy"), new IllegalArgumentException("dummy"))) {
            for (RestRequest request : List.of(
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).build(),
                new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of("error_trace", Boolean.toString(true))).build()
            )) {
                for (RestChannel channel : List.of(new SimpleExceptionRestChannel(request), new DetailedExceptionRestChannel(request))) {
                    RestResponse response = new RestResponse(channel, exception);
                    if (exception instanceof ElasticsearchException) {
                        assertThat(response.status(), is(RestStatus.INTERNAL_SERVER_ERROR));
                    } else {
                        assertThat(response.status(), is(RestStatus.BAD_REQUEST));
                    }
                    boolean traceExists = request.paramAsBoolean("error_trace", ERROR_TRACE_DEFAULT) && channel.detailedErrorsEnabled();
                    if (traceExists) {
                        assertThat(response.content().utf8ToString(), containsString(ElasticsearchException.STACK_TRACE));
                    } else {
                        assertThat(response.content().utf8ToString(), not(containsString(ElasticsearchException.STACK_TRACE)));
                    }
                }
            }
        }
    }

    public void testGuessRootCause() throws IOException {
        RestRequest request = new FakeRestRequest();
        {
            Exception e = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
            RestResponse response = new RestResponse(new DetailedExceptionRestChannel(request), e);
            String text = response.content().utf8ToString();
            assertThat(text, containsString("""
                {"root_cause":[{"type":"exception","reason":"an error occurred reading data"}]"""));
        }
        {
            Exception e = new FileNotFoundException("/foo/bar");
            RestResponse response = new RestResponse(new DetailedExceptionRestChannel(request), e);
            String text = response.content().utf8ToString();
            assertThat(text, containsString("""
                {"root_cause":[{"type":"file_not_found_exception","reason":"/foo/bar"}]"""));
        }
    }

    public void testNullThrowable() throws Exception {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        RestResponse response = new RestResponse(channel, null);
        String text = response.content().utf8ToString();
        assertThat(text, containsString("\"type\":\"unknown\""));
        assertThat(text, containsString("\"reason\":\"unknown\""));
        assertThat(text, not(containsString("error_trace")));
    }

    public void testConvert() throws IOException {
        RestRequest request = new FakeRestRequest();
        RestChannel channel = new DetailedExceptionRestChannel(request);
        ShardSearchFailure failure = new ShardSearchFailure(
            new ParsingException(1, 2, "foobar", null),
            new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null)
        );
        ShardSearchFailure failure1 = new ShardSearchFailure(
            new ParsingException(1, 2, "foobar", null),
            new SearchShardTarget("node_1", new ShardId("foo", "_na_", 2), null)
        );
        SearchPhaseExecutionException ex = new SearchPhaseExecutionException(
            "search",
            "all shards failed",
            new ShardSearchFailure[] { failure, failure1 }
        );
        RestResponse response = new RestResponse(channel, new RemoteTransportException("foo", ex));
        String text = response.content().utf8ToString();
        String expected = """
            {
              "error": {
                "root_cause": [ { "type": "parsing_exception", "reason": "foobar", "line": 1, "col": 2 } ],
                "type": "search_phase_execution_exception",
                "reason": "all shards failed",
                "phase": "search",
                "grouped": true,
                "failed_shards": [
                  {
                    "shard": 1,
                    "index": "foo",
                    "node": "node_1",
                    "reason": {
                      "type": "parsing_exception",
                      "reason": "foobar",
                      "line": 1,
                      "col": 2
                    }
                  }
                ]
              },
              "status": 400
            }""";
        assertEquals(XContentHelper.stripWhitespace(expected), XContentHelper.stripWhitespace(text));
        String stackTrace = ExceptionsHelper.stackTrace(ex);
        assertThat(stackTrace, containsString("org.elasticsearch.common.ParsingException: foobar"));
    }

    public void testResponseWhenPathContainsEncodingError() throws IOException {
        final String path = "%a";
        final RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath(path).build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> RestUtils.decodeComponent(request.rawPath()));
        final RestChannel channel = new DetailedExceptionRestChannel(request);
        // if we try to decode the path, this will throw an IllegalArgumentException again
        final RestResponse response = new RestResponse(channel, e);
        assertNotNull(response.content());
        final String content = response.content().utf8ToString();
        assertThat(content, containsString("\"type\":\"illegal_argument_exception\""));
        assertThat(content, containsString("\"reason\":\"partial escape sequence at end of string: %a\""));
        assertThat(content, containsString("\"status\":" + 400));
    }

    public void testResponseWhenInternalServerError() throws IOException {
        final RestRequest request = new FakeRestRequest();
        final RestChannel channel = new DetailedExceptionRestChannel(request);
        final RestResponse response = new RestResponse(channel, new ElasticsearchException("simulated"));
        assertNotNull(response.content());
        final String content = response.content().utf8ToString();
        assertThat(content, containsString("\"type\":\"exception\""));
        assertThat(content, containsString("\"reason\":\"simulated\""));
        assertThat(content, containsString("\"status\":" + 500));
    }

    public void testErrorToAndFromXContent() throws IOException {
        final boolean detailed = randomBoolean();

        Exception original;
        ElasticsearchException cause = null;
        String reason;
        String type = "exception";
        RestStatus status = RestStatus.INTERNAL_SERVER_ERROR;
        boolean addHeadersOrMetadata = false;

        switch (randomIntBetween(0, 5)) {
            case 0 -> {
                original = new ElasticsearchException("ElasticsearchException without cause");
                if (detailed) {
                    addHeadersOrMetadata = randomBoolean();
                }
                reason = "ElasticsearchException without cause";
            }
            case 1 -> {
                original = new ElasticsearchException("ElasticsearchException with a cause", new FileNotFoundException("missing"));
                if (detailed) {
                    addHeadersOrMetadata = randomBoolean();
                    cause = new ElasticsearchException("Elasticsearch exception [type=file_not_found_exception, reason=missing]");
                }
                type = "exception";
                reason = "ElasticsearchException with a cause";
            }
            case 2 -> {
                original = new ResourceNotFoundException("ElasticsearchException with custom status");
                status = RestStatus.NOT_FOUND;
                if (detailed) {
                    addHeadersOrMetadata = randomBoolean();
                }
                type = "resource_not_found_exception";
                reason = "ElasticsearchException with custom status";
            }
            case 3 -> {
                TransportAddress address = buildNewFakeTransportAddress();
                original = new RemoteTransportException(
                    "remote",
                    address,
                    "action",
                    new ResourceAlreadyExistsException("ElasticsearchWrapperException with a cause that has a custom status")
                );
                status = RestStatus.BAD_REQUEST;
                type = "resource_already_exists_exception";
                reason = "ElasticsearchWrapperException with a cause that has a custom status";
            }
            case 4 -> {
                original = new RemoteTransportException(
                    "ElasticsearchWrapperException with a cause that has a special treatment",
                    new IllegalArgumentException("wrong")
                );
                status = RestStatus.BAD_REQUEST;
                type = "illegal_argument_exception";
                reason = "wrong";
            }
            case 5 -> {
                status = randomFrom(RestStatus.values());
                original = new ElasticsearchStatusException("ElasticsearchStatusException with random status", status);
                if (detailed) {
                    addHeadersOrMetadata = randomBoolean();
                }
                type = "status_exception";
                reason = "ElasticsearchStatusException with random status";
            }
            default -> throw new UnsupportedOperationException("Failed to generate random exception");
        }

        String message = "Elasticsearch exception [type=" + type + ", reason=" + reason + "]";
        ElasticsearchStatusException expected = new ElasticsearchStatusException(message, status, cause);

        if (addHeadersOrMetadata) {
            ElasticsearchException originalException = ((ElasticsearchException) original);
            if (randomBoolean()) {
                originalException.addBodyHeader("foo", "bar", "baz");
                expected.addBodyHeader("foo", "bar", "baz");
            }
            if (randomBoolean()) {
                originalException.addMetadata("es.metadata_0", "0");
                expected.addMetadata("es.metadata_0", "0");
            }
            if (randomBoolean()) {
                String resourceType = randomAlphaOfLength(5);
                String resourceId = randomAlphaOfLength(5);
                originalException.setResources(resourceType, resourceId);
                expected.setResources(resourceType, resourceId);
            }
            if (randomBoolean()) {
                originalException.setIndex("_index");
                expected.setIndex("_index");
            }
        }

        final XContentType xContentType = randomFrom(XContentType.values());

        Map<String, String> params = Collections.singletonMap("format", xContentType.queryParameter());
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params).build();
        RestChannel channel = detailed ? new DetailedExceptionRestChannel(request) : new SimpleExceptionRestChannel(request);

        RestResponse response = new RestResponse(channel, original);

        ElasticsearchException parsedError;
        try (XContentParser parser = createParser(xContentType.xContent(), response.content())) {
            parsedError = errorFromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(expected.status(), parsedError.status());
        assertDeepEquals(expected, parsedError);
    }

    public void testNoErrorFromXContent() throws IOException {
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            try (XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.values()).xContent())) {
                builder.startObject();
                builder.field("status", randomFrom(RestStatus.values()).getStatus());
                builder.endObject();

                try (XContentParser parser = createParser(builder.contentType().xContent(), BytesReference.bytes(builder))) {
                    errorFromXContent(parser);
                }
            }
        });
        assertEquals("Failed to parse elasticsearch status exception: no exception was found", e.getMessage());
    }

    private static ElasticsearchStatusException errorFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

        ElasticsearchException exception = null;
        RestStatus status = null;

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            }
            if (RestResponse.STATUS.equals(currentFieldName)) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    ensureExpectedToken(XContentParser.Token.VALUE_NUMBER, token, parser);
                    status = RestStatus.fromCode(parser.intValue());
                }
            } else {
                exception = ElasticsearchException.failureFromXContent(parser);
            }
        }

        if (exception == null) {
            throw new IllegalStateException("Failed to parse elasticsearch status exception: no exception was found");
        }

        ElasticsearchStatusException result = new ElasticsearchStatusException(exception.getMessage(), status, exception.getCause());
        for (String header : exception.getBodyHeaderKeys()) {
            result.addBodyHeader(header, exception.getBodyHeader(header));
        }
        for (String metadata : exception.getMetadataKeys()) {
            result.addMetadata(metadata, exception.getMetadata(metadata));
        }
        return result;
    }

    public void testResponseContentTypeUponException() throws Exception {
        String mediaType = XContentType.VND_JSON.toParsedMediaType()
            .responseContentTypeHeader(
                Map.of(MediaType.COMPATIBLE_WITH_PARAMETER_NAME, String.valueOf(RestApiVersion.minimumSupported().major))
            );
        List<String> mediaTypeList = Collections.singletonList(mediaType);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withHeaders(Map.of("Accept", mediaTypeList)).build();
        RestChannel channel = new SimpleExceptionRestChannel(request);

        Exception t = new ElasticsearchException("an error occurred reading data", new FileNotFoundException("/foo/bar"));
        RestResponse response = new RestResponse(channel, t);
        assertThat(response.contentType(), equalTo(mediaType));
        assertWarnings(
            "The JSON format of non-detailed errors has changed in Elasticsearch 9.0 to match the JSON structure used for detailed errors."
        );
    }

    public void testSupressedLogging() throws IOException {
        final RestRequest request = new FakeRestRequest();
        final RestChannel channel = new DetailedExceptionRestChannel(request);
        // setting "rest.exception.stacktrace.skip" to true shouldn't change the default behaviour
        if (randomBoolean()) {
            request.params().put(REST_EXCEPTION_SKIP_STACK_TRACE, "true");
        }
        assertLogging(channel, new ElasticsearchException("simulated"), Level.WARN, "500", "simulated");
        assertLogging(channel, new IllegalArgumentException("simulated_iae"), Level.DEBUG, "400", "simulated_iae");
        assertLogging(channel, null, null, null, null);
        assertLogging(
            channel,
            new ElasticsearchSecurityException("unauthorized", RestStatus.UNAUTHORIZED),
            Level.DEBUG,
            "401",
            "unauthorized"
        );

        // setting "error_trace" to true should not affect logging
        request.params().clear();
        request.params().put("error_trace", "true");
        assertLogging(channel, new ElasticsearchException("simulated"), Level.WARN, "500", "simulated");
        assertLogging(
            channel,
            new ElasticsearchSecurityException("unauthorized", RestStatus.UNAUTHORIZED),
            Level.DEBUG,
            "401",
            "unauthorized"
        );
    }

    public void testSuppressedLoggingRecordsRootCause() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(
            new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/_search").build()
        );

        new RestResponse(channel, new ElasticsearchException("outer", new IllegalStateException("inner")));

        final Map<String, ?> fields = lastLoggedFields();
        assertEquals(IllegalStateException.class.getName(), fields.get("elasticsearch.error.root_cause.type"));
        assertEquals("inner", fields.get("elasticsearch.error.root_cause.message"));
        assertEquals("/my-index/_search", fields.get("url.path"));
        assertEquals(500, fields.get("http.response.status_code"));
        assertFalse(fields.containsKey("elasticsearch.error.index"));
        assertFalse(fields.containsKey("elasticsearch.error.shard"));
    }

    public void testSuppressedLoggingOmitsAbsentRootCauseMessage() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());

        new RestResponse(channel, new ElasticsearchException("outer", new IllegalStateException()));

        assertFalse(lastLoggedFields().containsKey("elasticsearch.error.root_cause.message"));
    }

    public void testSuppressedLoggingRecordsIndexAndShard() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());

        new RestResponse(channel, new ElasticsearchException("outer", new ShardNotFoundException(new ShardId("my-index", "uuid", 3))));

        final Map<String, ?> fields = lastLoggedFields();
        assertEquals("my-index", fields.get("elasticsearch.error.index"));
        assertEquals(3, fields.get("elasticsearch.error.shard"));
    }

    public void testSuppressedLoggingRecordsIndexWhenRootCauseIsNotScoped() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());
        final ShardId shardId = new ShardId("my-index", "uuid", 3);

        new RestResponse(channel, new ShardNotFoundException(shardId, "no such shard", new IllegalStateException("engine is closed")));

        final Map<String, ?> fields = lastLoggedFields();
        assertEquals(IllegalStateException.class.getName(), fields.get("elasticsearch.error.root_cause.type"));
        assertEquals("engine is closed", fields.get("elasticsearch.error.root_cause.message"));
        assertEquals("my-index", fields.get("elasticsearch.error.index"));
        assertEquals(3, fields.get("elasticsearch.error.shard"));
    }

    public void testSuppressedLoggingRecordsRootCauseBehindAllShardsFailed() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());
        final ShardId shardId = new ShardId("my-index", "uuid", 3);
        final ShardSearchFailure shardFailure = new ShardSearchFailure(
            new ShardNotFoundException(shardId),
            new SearchShardTarget("node", shardId, null)
        );

        new RestResponse(
            channel,
            new SearchPhaseExecutionException("query", "all shards failed", new ShardSearchFailure[] { shardFailure })
        );

        final Map<String, ?> fields = lastLoggedFields();
        assertEquals(ShardNotFoundException.class.getName(), fields.get("elasticsearch.error.root_cause.type"));
        assertEquals("my-index", fields.get("elasticsearch.error.index"));
    }

    public void testSuppressedLoggingRecordsHandlerName() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest()) {
            @Override
            public String handlerName() {
                return "search_action";
            }
        };

        new RestResponse(channel, new ElasticsearchException("simulated"));

        assertEquals("search_action", lastLoggedFields().get("elasticsearch.rest.handler"));
    }

    public void testSuppressedLoggingOmitsUnknownHandlerName() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());

        new RestResponse(channel, new ElasticsearchException("simulated"));

        assertFalse(lastLoggedFields().containsKey("elasticsearch.rest.handler"));
    }

    public void testSuppressedLoggingWithTwoDifferentShardFailures() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());
        final ShardSearchFailure[] failures = new ShardSearchFailure[] {
            new ShardSearchFailure(
                new IllegalStateException("engine is closed"),
                new SearchShardTarget("node-0", new ShardId("my-index", "uuid", 0), null)
            ),
            new ShardSearchFailure(
                new IllegalArgumentException("bad argument"),
                new SearchShardTarget("node-1", new ShardId("other-index", "uuid", 1), null)
            ) };

        new RestResponse(channel, new SearchPhaseExecutionException("query", "all shards failed", failures));

        final Map<String, ?> fields = lastLoggedFields();
        // NOTE: guessFirstRootCause returns on the first shard failure, so the remaining failures reach error.stack_trace only
        assertEquals(IllegalStateException.class.getName(), fields.get("elasticsearch.error.root_cause.type"));
        assertEquals("engine is closed", fields.get("elasticsearch.error.root_cause.message"));
        // NOTE: the cause chain carries no index here, so both are read from the first ShardSearchFailure
        assertEquals("my-index", fields.get("elasticsearch.error.index"));
        assertEquals(0, fields.get("elasticsearch.error.shard"));
    }

    public void testSuppressedLoggingKeepsPlainTextMessage() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(
            new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/_search").build()
        );

        new RestResponse(channel, new ElasticsearchException("outer", new IllegalStateException("inner")));

        final PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").withAlwaysWriteExceptions(false).build();
        assertEquals("path: /my-index/_search, params: {}, status: 500", layout.toSerializable(appender.getLastEventAndReset()));
    }

    public void testSuppressedLoggingRecordsWrappedSearchFailureLocation() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());
        final ShardSearchFailure shardFailure = new ShardSearchFailure(
            new IllegalStateException("engine is closed"),
            new SearchShardTarget("node", new ShardId("my-index", "uuid", 3), null)
        );
        final SearchPhaseExecutionException searchFailure = new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] { shardFailure }
        );

        new RestResponse(channel, new RemoteTransportException("error while communicating with remote cluster [remote]", searchFailure));

        final Map<String, ?> fields = lastLoggedFields();
        assertEquals(IllegalStateException.class.getName(), fields.get("elasticsearch.error.root_cause.type"));
        assertEquals("my-index", fields.get("elasticsearch.error.index"));
        assertEquals(3, fields.get("elasticsearch.error.shard"));
    }

    public void testSuppressedLoggingOmitsLocationForCoordinatorFailure() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());
        final ShardSearchFailure shardFailure = new ShardSearchFailure(
            new IllegalArgumentException("unrelated shard failure"),
            new SearchShardTarget("node", new ShardId("my-index", "uuid", 3), null)
        );

        new RestResponse(
            channel,
            new SearchPhaseExecutionException(
                "fetch",
                "Phase failed",
                new IllegalStateException("coordinator failure"),
                new ShardSearchFailure[] { shardFailure }
            )
        );

        final Map<String, ?> fields = lastLoggedFields();
        assertEquals("coordinator failure", fields.get("elasticsearch.error.root_cause.message"));
        assertFalse(fields.containsKey("elasticsearch.error.index"));
        assertFalse(fields.containsKey("elasticsearch.error.shard"));
    }

    public void testSuppressedLoggingRecordsIndexWithoutShard() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(new FakeRestRequest());

        new RestResponse(channel, new IndexClosedException(new Index("my-index", "uuid")));

        final Map<String, ?> fields = lastLoggedFields();
        assertEquals("my-index", fields.get("elasticsearch.error.index"));
        assertFalse(fields.containsKey("elasticsearch.error.shard"));
    }

    public void testSuppressedLoggingSerialisesToEcsJson() throws IOException {
        final RestChannel channel = new DetailedExceptionRestChannel(
            new FakeRestRequest.Builder(xContentRegistry()).withPath("/my-index/_search").build()
        );
        final String awkward = "quote \" backslash \\ newline \n end";

        new RestResponse(
            channel,
            new ElasticsearchException("outer", new ShardNotFoundException(new ShardId("my-index", "uuid", 3), awkward))
        );

        final EcsLayout layout = EcsLayout.newBuilder()
            .setConfiguration(LoggerContext.getContext(false).getConfiguration())
            .setEventDataset("elasticsearch.server")
            .build();
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), layout.toSerializable(appender.getLastEventAndReset()))) {
            final Map<String, Object> fields = parser.map();
            assertEquals("path: /my-index/_search, params: {}, status: 500", fields.get("message"));
            assertEquals(ElasticsearchException.class.getName(), fields.get("error.type"));
            assertEquals("outer", fields.get("error.message"));
            assertEquals(500, fields.get("http.response.status_code"));
            assertEquals(ShardNotFoundException.class.getName(), fields.get("elasticsearch.error.root_cause.type"));
            assertEquals(awkward, fields.get("elasticsearch.error.root_cause.message"));
            assertEquals(3, fields.get("elasticsearch.error.shard"));
            assertFalse(fields.containsKey("elasticsearch.rest.handler"));
        }
    }

    public void testWalkCauseChain() {
        final RuntimeException root = new RuntimeException("root");
        assertThat(RestResponse.walkCauseChain(root).deepest(), sameInstance(root));
        assertNull(RestResponse.walkCauseChain(root).indexScoped());

        Throwable wrapped = root;
        for (int i = 0; i < randomIntBetween(1, 32); i++) {
            wrapped = new RuntimeException("wrapper", wrapped);
        }
        assertThat(RestResponse.walkCauseChain(wrapped).deepest(), sameInstance(root));
    }

    public void testWalkCauseChainIgnoresSuppressed() {
        final RuntimeException root = new RuntimeException("root");
        final RuntimeException outer = new RuntimeException("outer", root);
        outer.addSuppressed(new ShardNotFoundException(new ShardId("suppressed-index", "uuid", 7)));

        final RestResponse.CauseChain chain = RestResponse.walkCauseChain(outer);

        assertThat(chain.deepest(), sameInstance(root));
        assertNull(chain.indexScoped());
    }

    public void testWalkCauseChainOfCycle() {
        final RuntimeException e1 = new RuntimeException();
        final RuntimeException e2 = new RuntimeException(e1);
        e1.initCause(e2);
        assertThat(RestResponse.walkCauseChain(e1).deepest(), sameInstance(e2));
        assertThat(RestResponse.walkCauseChain(e2).deepest(), sameInstance(e1));
    }

    public void testWalkCauseChainFindsDeepestIndexScopedCause() {
        final ShardNotFoundException deep = new ShardNotFoundException(
            new ShardId("deep-index", "uuid", 3),
            "no such shard",
            new IllegalStateException("engine is closed")
        );
        final ShardNotFoundException shallow = new ShardNotFoundException(new ShardId("shallow-index", "uuid", 0), "no such shard", deep);

        final RestResponse.CauseChain chain = RestResponse.walkCauseChain(new RuntimeException("outer", shallow));

        assertThat(chain.deepest(), instanceOf(IllegalStateException.class));
        assertThat(chain.indexScoped(), sameInstance(deep));
    }

    public void testSuppressedLoggingKeepsFailureLocationAcrossTransport() throws IOException {
        final ShardSearchFailure shardFailure = new ShardSearchFailure(
            new IllegalStateException("engine is closed"),
            new SearchShardTarget("node", new ShardId("my-index", "uuid", 3), null)
        );
        final SearchPhaseExecutionException searchFailure = new SearchPhaseExecutionException(
            "query",
            "all shards failed",
            new ShardSearchFailure[] { shardFailure }
        );
        final Exception received;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeException(new RemoteTransportException("remote search failure", searchFailure));
            try (StreamInput in = out.bytes().streamInput()) {
                received = in.readException();
            }
        }

        new RestResponse(new DetailedExceptionRestChannel(new FakeRestRequest()), received);

        final Map<String, ?> fields = lastLoggedFields();
        assertEquals(IllegalStateException.class.getName(), fields.get("elasticsearch.error.root_cause.type"));
        assertEquals("engine is closed", fields.get("elasticsearch.error.root_cause.message"));
        assertEquals("my-index", fields.get("elasticsearch.error.index"));
        assertEquals(3, fields.get("elasticsearch.error.shard"));
    }

    private Map<String, ?> lastLoggedFields() {
        final LogEvent logEvent = appender.getLastEventAndReset();
        assertThat(logEvent.getMessage(), instanceOf(MapMessage.class));
        return ((MapMessage<?, ?>) logEvent.getMessage()).getData();
    }

    private void assertLogging(
        RestChannel channel,
        Exception exception,
        Level expectedLogLevel,
        String expectedStatus,
        String expectedExceptionMessage
    ) throws IOException {
        RestResponse response = new RestResponse(channel, exception);
        assertNotNull(response.content());
        LogEvent logEvent = appender.getLastEventAndReset();
        if (expectedLogLevel != null) {
            assertThat(logEvent.getLevel(), is(expectedLogLevel));
            assertThat(
                logEvent.getMessage().getFormattedMessage(),
                is("path: , params: " + channel.request().params() + ", status: " + expectedStatus)
            );
            assertThat(logEvent.getThrown().getMessage(), is(expectedExceptionMessage));
            assertThat(logEvent.getLoggerName(), is("rest.suppressed"));
        } else {
            assertNull(logEvent);
        }
    }

    public static class WithHeadersException extends ElasticsearchException {

        WithHeadersException() {
            super("");
            this.addBodyHeader("n1", "v11", "v12");
            this.addBodyHeader("n2", "v21", "v22");
            this.addHttpHeader("My-Header", "v1");
            this.addMetadata("es.test", "value1", "value2");
        }

        @Override
        public RestStatus status() {
            return RestStatus.BAD_REQUEST;
        }
    }

    private static class SimpleExceptionRestChannel extends AbstractRestChannel {

        SimpleExceptionRestChannel(RestRequest request) {
            super(request, false);
        }

        @Override
        public void sendResponse(RestResponse response) {}
    }

    private static class DetailedExceptionRestChannel extends AbstractRestChannel {

        DetailedExceptionRestChannel(RestRequest request) {
            super(request, true);
        }

        @Override
        public void sendResponse(RestResponse response) {}
    }
}
