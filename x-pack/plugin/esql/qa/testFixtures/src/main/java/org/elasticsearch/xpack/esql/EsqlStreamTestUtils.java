/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Shared utilities for testing the ES|QL streaming API ({@code POST /_query/stream}).
 * The main entry point is {@link #stream}, which drives an incremental NDJSON consumer
 * over a real HTTP connection, handing each complete line to a {@link StreamGate}.
 */
public final class EsqlStreamTestUtils {

    private EsqlStreamTestUtils() {}

    public enum Terminal {
        FOOTER,
        ERROR,
        NONE
    }

    public record StreamOutcome(
        Integer httpStatus,
        String contentType,
        List<Map<String, Object>> lines,
        Terminal terminal,
        long rowCount,
        Exception transportFailure,
        boolean truncated,
        boolean clientAborted
    ) {
        public Map<String, Object> terminalLine() {
            return lines.isEmpty() ? null : lines.get(lines.size() - 1);
        }

        public Map<String, Object> error() {
            Map<String, Object> last = terminalLine();
            if (last == null) {
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> err = (Map<String, Object>) last.get("error");
            return err;
        }

        public String errorType() {
            Map<String, Object> e = error();
            return e == null ? null : (String) e.get("type");
        }

        public boolean isPartial() {
            Map<String, Object> last = terminalLine();
            if (last == null) {
                return false;
            }
            return Boolean.TRUE.equals(last.get("is_partial"));
        }

        @SuppressWarnings("unchecked")
        public List<List<Object>> rows() {
            List<List<Object>> result = new ArrayList<>();
            for (Map<String, Object> line : lines) {
                Object values = line.get("values");
                if (values instanceof List<?> pageRows) {
                    for (Object row : pageRows) {
                        result.add((List<Object>) row);
                    }
                }
            }
            return result;
        }
    }

    @FunctionalInterface
    public interface StreamGate {
        void onLine(int lineIndex, Map<String, Object> line, StreamControl control) throws IOException;
    }

    public interface StreamControl {
        void suspendInput();

        void requestInput();

        void abort() throws IOException;
    }

    public static StreamOutcome stream(RestClient client, String queryJsonBody, StreamGate gate, String... queryParams) throws Exception {
        String path = "/_query/stream";
        if (queryParams.length > 0) {
            path += "?" + String.join("&", queryParams);
        }
        Request request = new Request("POST", path);
        request.setJsonEntity(queryJsonBody);

        AtomicReference<StreamingNdjsonConsumer> consumerRef = new AtomicReference<>();
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).setHttpAsyncResponseConsumerFactory(() -> {
                StreamingNdjsonConsumer consumer = new StreamingNdjsonConsumer(gate);
                consumerRef.set(consumer);
                return consumer;
            }).build()
        );

        Exception transportFailure = null;
        try {
            client.performRequest(request);
        } catch (Exception e) {
            transportFailure = e;
        }

        StreamingNdjsonConsumer c = consumerRef.get();
        if (c == null) {
            return new StreamOutcome(null, null, List.of(), Terminal.NONE, 0, transportFailure, false, false);
        }
        return c.buildOutcome(transportFailure);
    }

    public static Response rawStream(RestClient client, String bodyJson, String... queryParams) throws IOException {
        String path = "/_query/stream";
        if (queryParams.length > 0) {
            path += "?" + String.join("&", queryParams);
        }
        Request request = new Request("POST", path);
        request.setJsonEntity(bodyJson);
        tolerateDefaultLimitWarning(request);
        return client.performRequest(request);
    }

    public static List<Map<String, Object>> parseNdjson(Response response) throws IOException {
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        List<Map<String, Object>> result = new ArrayList<>();
        for (String line : body.split("\n")) {
            if (line.isBlank() == false) {
                result.add(XContentHelper.convertToMap(XContentType.JSON.xContent(), line, false));
            }
        }
        return result;
    }

    public static final String DEFAULT_LIMIT_WARNING = "No limit defined, adding default limit of [1000]";

    public static void tolerateDefaultLimitWarning(Request request) {
        request.setOptions(request.getOptions().toBuilder().setWarningsHandler(warnings -> {
            List<String> unexpected = new ArrayList<>(warnings);
            unexpected.remove(DEFAULT_LIMIT_WARNING);
            return unexpected.isEmpty() == false;
        }));
    }

    public static void assertStreamInvariants(StreamOutcome outcome, boolean allowPartial, long limit) {
        if (outcome.httpStatus() == null) {
            assertNotNull("No HTTP status received and no transport-level failure — request vanished silently", outcome.transportFailure());
            return;
        }

        assertThat("Content-Type header must be present", outcome.contentType(), notNullValue());
        assertThat(
            "Content-Type was ["
                + outcome.contentType()
                + "] not NDJSON — the request was likely rejected upstream of EsqlStreamResponseListener"
                + " (strict body parse, auth, or RestController). httpStatus="
                + outcome.httpStatus()
                + ", firstLine="
                + (outcome.lines().isEmpty() ? "<none>" : outcome.lines().get(0)),
            outcome.contentType(),
            containsString("application/x-ndjson")
        );

        if (outcome.httpStatus() != 200) {
            assertThat(
                "Non-200 response must produce exactly one NDJSON line for HTTP status " + outcome.httpStatus(),
                outcome.lines(),
                hasSize(1)
            );
            Map<String, Object> errorLine = outcome.lines().get(0);
            assertThat("Non-200 response line must contain 'error'", errorLine, hasKey("error"));
            assertThat("Non-200 response line must contain 'status'", errorLine, hasKey("status"));
            int statusInBody = ((Number) errorLine.get("status")).intValue();
            if (statusInBody != outcome.httpStatus()) {
                fail("In-body 'status' " + statusInBody + " does not match HTTP status " + outcome.httpStatus());
            }
            assertFalse("Body must not be truncated for a non-200 (pre-stream) error response", outcome.truncated());
            assertNoDisallowedErrorType(outcome);
            return;
        }

        List<Map<String, Object>> lines = outcome.lines();

        if (lines.isEmpty()) {
            if (outcome.clientAborted() == false && outcome.transportFailure() == null) {
                fail("200 response with no lines and no client abort or transport failure");
            }
            return;
        }

        Map<String, Object> columnsLine = lines.get(0);
        assertThat("First line must contain 'columns'", columnsLine, hasKey("columns"));
        assertThat(columnsLine, not(hasKey("values")));
        assertThat(columnsLine, not(hasKey("took")));
        assertThat(columnsLine, not(hasKey("error")));
        assertThat("'columns' key must appear exactly once (line 0)", lines.subList(1, lines.size()), everyItem(not(hasKey("columns"))));

        int terminalIdx = -1;
        for (int i = 1; i < lines.size(); i++) {
            Map<String, Object> line = lines.get(i);
            if (line.containsKey("took") || line.containsKey("error")) {
                terminalIdx = i;
                break;
            }
            if (line.containsKey("values") == false) {
                fail("Line " + i + " is neither a values line nor a terminal line: " + line.keySet());
            }
        }

        if (terminalIdx != -1) {
            assertThat("Lines appeared after the terminal line at index " + terminalIdx, lines, hasSize(terminalIdx + 1));
            assertFalse("Body must not be truncated after the terminal line", outcome.truncated());
        }

        if (outcome.terminal() == Terminal.NONE) {
            if (outcome.clientAborted() == false && outcome.transportFailure() == null) {
                fail("200 response ended with no terminal line and no client abort or transport failure — streaming protocol violation");
            }
        }

        if (outcome.terminal() == Terminal.FOOTER) {
            Map<String, Object> footer = outcome.terminalLine();
            assertThat("Footer must contain 'took'", footer, hasKey("took"));
            assertThat("Footer 'took' must be a Number", footer.get("took"), instanceOf(Number.class));
            if (outcome.isPartial() && allowPartial == false) {
                fail("is_partial=true arrived but allow_partial_results was not set");
            }
            Object warnings = footer.get("warnings");
            if (warnings != null) {
                assertThat("Footer 'warnings' must be a list", warnings, instanceOf(List.class));
                assertThat("Footer 'warnings', when present, must be non-empty", (List<?>) warnings, not(empty()));
            }
        }

        if (outcome.terminal() == Terminal.ERROR) {
            assertNoDisallowedErrorType(outcome);
        }

        assertThat("rowCount exceeds query LIMIT — a page was likely double-delivered", outcome.rowCount(), lessThanOrEqualTo(limit));
    }

    private static void assertNoDisallowedErrorType(StreamOutcome outcome) {
        Map<String, Object> error = outcome.error();
        if (error == null) {
            return;
        }
        String type = (String) error.get("type");
        String reason = (String) error.get("reason");

        if ("task_cancelled_exception".equals(type)) {
            fail("Cancellation exceptions must not surface as the top-level error type: " + error);
        }
        if (reason != null && reason.contains("TaskCancelledException")) {
            fail("TaskCancelledException must not appear in error reason: " + reason);
        }
        if ("remote_transport_exception".equals(type)) {
            fail("RemoteTransportException must be unwrapped before reporting: " + error);
        }
        if (reason != null && reason.startsWith("RemoteTransportException")) {
            fail("Error reason must not start with RemoteTransportException: " + reason);
        }
        if ("x_content_parse_exception".equals(type) || "parsing_exception".equals(type)) {
            fail(
                "Request was malformed, not disrupted — the body or params are wrong for /_query/stream"
                    + " (check that allow_partial_results and other non-body params are sent as URL params, not body fields): "
                    + error
            );
        }
    }

    private static final class StreamingNdjsonConsumer implements HttpAsyncResponseConsumer<HttpResponse>, StreamControl {

        private final StreamGate gate;
        private final List<Map<String, Object>> lines = new ArrayList<>();
        private final StringBuilder lineBuffer = new StringBuilder();

        private HttpResponse receivedResponse;
        private IOControl ioControl;

        private Integer httpStatus;
        private String contentType;
        private long rowCount;
        private Terminal terminal = Terminal.NONE;
        private boolean truncated;
        private volatile boolean clientAborted;
        private boolean done;

        StreamingNdjsonConsumer(StreamGate gate) {
            this.gate = gate;
        }

        @Override
        public void responseReceived(HttpResponse response) {
            receivedResponse = response;
            httpStatus = response.getStatusLine().getStatusCode();
            Header ct = response.getFirstHeader("Content-Type");
            if (ct != null) {
                contentType = ct.getValue();
            }
        }

        @Override
        public void consumeContent(ContentDecoder decoder, IOControl ioControl) throws IOException {
            this.ioControl = ioControl;
            if (clientAborted) {
                done = true;
                return;
            }
            ByteBuffer buf = ByteBuffer.allocate(8192);
            int n = decoder.read(buf);
            if (n > 0) {
                buf.flip();
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                String chunk = new String(bytes, StandardCharsets.UTF_8);
                int start = 0;
                int nl;
                while ((nl = chunk.indexOf('\n', start)) >= 0) {
                    lineBuffer.append(chunk, start, nl);
                    String raw = lineBuffer.toString().trim();
                    lineBuffer.setLength(0);
                    start = nl + 1;
                    if (raw.isEmpty() == false) {
                        Map<String, Object> line = XContentHelper.convertToMap(XContentType.JSON.xContent(), raw, false);
                        int lineIndex = lines.size();
                        lines.add(line);

                        Object values = line.get("values");
                        if (values instanceof List<?> rows) {
                            rowCount += rows.size();
                        }

                        if (line.containsKey("took")) {
                            terminal = Terminal.FOOTER;
                        } else if (line.containsKey("error")) {
                            terminal = Terminal.ERROR;
                        }

                        if (gate != null) {
                            gate.onLine(lineIndex, line, this);
                            if (clientAborted) {
                                break;
                            }
                        }
                    }
                }
                if (start < chunk.length()) {
                    lineBuffer.append(chunk, start, chunk.length());
                }
            }
            if (clientAborted) {
                lineBuffer.setLength(0);
                done = true;
            } else if (decoder.isCompleted()) {
                truncated = lineBuffer.length() > 0;
                lineBuffer.setLength(0);
                done = true;
            }
        }

        @Override
        public void responseCompleted(HttpContext context) {
            done = true;
        }

        @Override
        public void failed(Exception ex) {
            done = true;
        }

        @Override
        public Exception getException() {
            return null;
        }

        @Override
        public HttpResponse getResult() {
            return receivedResponse;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public void close() {}

        @Override
        public boolean cancel() {
            return false;
        }

        @Override
        public void suspendInput() {
            ioControl.suspendInput();
        }

        @Override
        public void requestInput() {
            ioControl.requestInput();
        }

        @Override
        public void abort() throws IOException {
            clientAborted = true;
            ioControl.shutdown();
        }

        StreamOutcome buildOutcome(Exception transportFailure) {
            return new StreamOutcome(
                httpStatus,
                contentType,
                List.copyOf(lines),
                terminal,
                rowCount,
                transportFailure,
                truncated,
                clientAborted
            );
        }
    }
}
