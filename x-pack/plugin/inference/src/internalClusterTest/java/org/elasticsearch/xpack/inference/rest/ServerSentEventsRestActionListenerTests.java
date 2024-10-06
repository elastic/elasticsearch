/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.util.SimpleInputBuffer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEvent;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventField;
import org.elasticsearch.xpack.inference.external.response.streaming.ServerSentEventParser;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class ServerSentEventsRestActionListenerTests extends ESIntegTestCase {
    private static final String INFERENCE_ROUTE = "/_inference";
    private static final String REQUEST_COUNT = "request_count";
    private static final String WITH_ERROR = "with_error";
    private static final String ERROR_ROUTE = "/_inference_error";
    private static final String NO_STREAM_ROUTE = "/_inference_no_stream";
    private static final Exception expectedException = new IllegalStateException("hello there");
    private static final String expectedExceptionAsServerSentEvent = """
        {\
        "error":{"root_cause":[{"type":"illegal_state_exception","reason":"hello there",\
        "caused_by":{"type":"illegal_state_exception","reason":"hello there"}}],\
        "type":"illegal_state_exception","reason":"hello there"},"status":500\
        }""";

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), StreamingPlugin.class);
    }

    public static class StreamingPlugin extends Plugin implements ActionPlugin {
        @Override
        public Collection<RestHandler> getRestHandlers(
            Settings settings,
            NamedWriteableRegistry namedWriteableRegistry,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<DiscoveryNodes> nodesInCluster,
            Predicate<NodeFeature> clusterSupportsFeature
        ) {
            return List.of(new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.POST, INFERENCE_ROUTE));
                }

                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                    var requestCount = request.paramAsInt(REQUEST_COUNT, -1);
                    assert requestCount >= 0;
                    var withError = request.paramAsBoolean(WITH_ERROR, false);
                    var publisher = new RandomPublisher(requestCount, withError);
                    var inferenceServiceResults = new StreamingInferenceServiceResults(publisher);
                    var inferenceResponse = new InferenceAction.Response(inferenceServiceResults, inferenceServiceResults.publisher());
                    new ServerSentEventsRestActionListener(channel).onResponse(inferenceResponse);
                }
            }, new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.POST, ERROR_ROUTE));
                }

                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                    new ServerSentEventsRestActionListener(channel).onFailure(expectedException);
                }
            }, new RestHandler() {
                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.POST, NO_STREAM_ROUTE));
                }

                @Override
                public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                    var inferenceResponse = new InferenceAction.Response(new SingleInferenceServiceResults());
                    new ServerSentEventsRestActionListener(channel).onResponse(inferenceResponse);
                }
            });
        }
    }

    private static class StreamingInferenceServiceResults implements InferenceServiceResults {
        private final Flow.Publisher<ChunkedToXContent> publisher;

        private StreamingInferenceServiceResults(Flow.Publisher<ChunkedToXContent> publisher) {
            this.publisher = publisher;
        }

        @Override
        public Flow.Publisher<ChunkedToXContent> publisher() {
            return publisher;
        }

        @Override
        public List<? extends InferenceResults> transformToCoordinationFormat() {
            return List.of();
        }

        @Override
        public List<? extends InferenceResults> transformToLegacyFormat() {
            return List.of();
        }

        @Override
        public Map<String, Object> asMap() {
            return Map.of();
        }

        @Override
        public boolean isStreaming() {
            return true;
        }

        @Override
        public String getWriteableName() {
            return "StreamingInferenceServiceResults";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            throw new UnsupportedOperationException("Should not be called");
        }
    }

    private static class RandomPublisher implements Flow.Publisher<ChunkedToXContent> {
        private final int requestCount;
        private final boolean withError;

        private RandomPublisher(int requestCount, boolean withError) {
            this.requestCount = requestCount;
            this.withError = withError;
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ChunkedToXContent> subscriber) {
            var resultCount = new AtomicInteger(requestCount);
            subscriber.onSubscribe(new Flow.Subscription() {
                @Override
                public void request(long n) {
                    if (resultCount.getAndDecrement() > 0) {
                        subscriber.onNext(new RandomString());
                    } else if (withError) {
                        subscriber.onError(expectedException);
                    } else {
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {
                    resultCount.set(Integer.MIN_VALUE);
                }
            });
        }
    }

    private static class RandomString implements ChunkedToXContent {
        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            var randomString = randomUnicodeOfLengthBetween(2, 20);
            return ChunkedToXContent.builder(params).object(b -> b.field("delta", randomString));
        }
    }

    private static class SingleInferenceServiceResults implements InferenceServiceResults {

        @Override
        public List<? extends InferenceResults> transformToCoordinationFormat() {
            return List.of();
        }

        @Override
        public List<? extends InferenceResults> transformToLegacyFormat() {
            return List.of();
        }

        @Override
        public Map<String, Object> asMap() {
            return Map.of();
        }

        @Override
        public String getWriteableName() {
            return "";
        }

        @Override
        public void writeTo(StreamOutput out) {

        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return ChunkedToXContent.builder(params).field("result", randomUnicodeOfLengthBetween(2, 20));
        }
    }

    /**
     * We can use Elasticsearch's Java SDK to verify that we are sending bytes as we get them and that the response can be processed and
     * validated before the Response object is returned from the client.
     */
    private static class AsyncResponseConsumer extends AbstractAsyncResponseConsumer<HttpResponse> {
        private final RandomStringCollector collector;
        private final AtomicReference<HttpResponse> response = new AtomicReference<>();

        private AsyncResponseConsumer(RandomStringCollector collector) {
            this.collector = collector;
        }

        @Override
        protected void onResponseReceived(HttpResponse httpResponse) {
            response.set(httpResponse);
        }

        @Override
        protected void onContentReceived(ContentDecoder contentDecoder, IOControl ioControl) throws IOException {
            var buffer = new SimpleInputBuffer(4096);
            var consumed = buffer.consumeContent(contentDecoder);
            var allBytes = new byte[consumed];
            buffer.read(allBytes);

            var response = new String(allBytes, StandardCharsets.UTF_8);
            try {
                collector.collect(response);
            } catch (IOException e) {
                ioControl.shutdown();
                throw e;
            }
        }

        @Override
        protected void onEntityEnclosed(HttpEntity httpEntity, ContentType contentType) {

        }

        @Override
        protected HttpResponse buildResult(HttpContext httpContext) {
            return response.get();
        }

        @Override
        protected void releaseResources() {}
    }

    private static class RandomStringCollector {
        private final Deque<String> stringsVerified = new LinkedBlockingDeque<>();
        private final ServerSentEventParser sseParser = new ServerSentEventParser();

        private void collect(String str) throws IOException {
            sseParser.parse(str.getBytes(StandardCharsets.UTF_8))
                .stream()
                .filter(event -> event.name() == ServerSentEventField.DATA)
                .filter(ServerSentEvent::hasValue)
                .map(ServerSentEvent::value)
                .forEach(stringsVerified::offer);
        }
    }

    public void testResponse() {
        var collector = new RandomStringCollector();
        var expectedTestCount = randomIntBetween(2, 30);
        var request = new Request(RestRequest.Method.POST.name(), INFERENCE_ROUTE);
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setHttpAsyncResponseConsumerFactory(() -> new AsyncResponseConsumer(collector))
                .addParameter(REQUEST_COUNT, String.valueOf(expectedTestCount))
                .build()
        );

        var response = callAsync(request);
        assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK));
        assertThat(collector.stringsVerified.size(), equalTo(expectedTestCount + 1)); // normal payload count + done byte
        assertThat(collector.stringsVerified.peekLast(), equalTo("[DONE]"));
    }

    private Response callAsync(Request request) {
        var response = new AtomicReference<Response>();
        var exception = new AtomicReference<Exception>();
        var countdown = new CountDownLatch(1);
        var cancellable = getRestClient().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response r) {
                response.set(r);
                countdown.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exception.set(e);
                countdown.countDown();
            }
        });

        try {
            if (countdown.await(10, TimeUnit.SECONDS) == false) {
                cancellable.cancel();
                fail("Failed to finish test in 10 seconds");
            }
        } catch (InterruptedException e) {
            cancellable.cancel();
            fail(e, "Failed to finish test");
        }

        assertThat("No exceptions should be thrown.", exception.get(), nullValue());
        assertThat(response.get(), notNullValue());
        return response.get();
    }

    public void testOnFailure() throws IOException {
        var request = new Request(RestRequest.Method.POST.name(), ERROR_ROUTE);

        try {
            getRestClient().performRequest(request);
            fail("Expected an exception to be thrown from the error route");
        } catch (ResponseException e) {
            var response = e.getResponse();
            assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_INTERNAL_SERVER_ERROR));
            assertThat(EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8), equalTo("""
                \uFEFFevent: error
                data:\s""" + expectedExceptionAsServerSentEvent + "\n\n"));
        }
    }

    public void testErrorMidStream() {
        var collector = new RandomStringCollector();
        var expectedTestCount = randomIntBetween(2, 30);
        var request = new Request(RestRequest.Method.POST.name(), INFERENCE_ROUTE);
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setHttpAsyncResponseConsumerFactory(() -> new AsyncResponseConsumer(collector))
                .addParameter(REQUEST_COUNT, String.valueOf(expectedTestCount))
                .addParameter(WITH_ERROR, String.valueOf(true))
                .build()
        );

        var response = callAsync(request);
        assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK)); // error still starts with 200-OK
        assertThat(collector.stringsVerified.size(), equalTo(expectedTestCount + 1)); // normal payload count + last error byte
        assertThat("DONE chunk is not sent on error", collector.stringsVerified.stream().anyMatch("[DONE]"::equals), equalTo(false));
        assertThat(collector.stringsVerified.getLast(), equalTo(expectedExceptionAsServerSentEvent));
    }

    public void testNoStream() throws IOException {
        var pattern = Pattern.compile("^\uFEFFevent: message\ndata: \\{\"result\":\".*\"}\n\n\uFEFFevent: message\ndata: \\[DONE]\n\n$");
        var request = new Request(RestRequest.Method.POST.name(), NO_STREAM_ROUTE);
        var response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(HttpStatus.SC_OK));
        var responseString = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

        assertThat(
            "Expected " + responseString + " to match pattern " + pattern.pattern(),
            pattern.matcher(responseString).matches(),
            is(true)
        );
    }
}
