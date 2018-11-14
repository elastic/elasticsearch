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
package org.elasticsearch.client.watcher;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.mapper.DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

public class GetWatchResponseTests extends AbstractXContentTestCase<GetWatchResponseTests.ResponseWrapper> {
    /**
     * A wrapper for {@link GetWatchResponse} that is able to serialize the response with {@link ToXContent}.
     */
    static class ResponseWrapper implements ToXContentObject {
        final NamedXContentRegistry xContentRegistry;
        final GetWatchResponse response;

        ResponseWrapper(NamedXContentRegistry xContentRegistry, GetWatchResponse response) {
            this.xContentRegistry = xContentRegistry;
            this.response = response;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return GetWatchResponseTests.toXContent(xContentRegistry, response, builder);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResponseWrapper that = (ResponseWrapper) o;
            return Objects.equals(response, that.response);
        }

        @Override
        public int hashCode() {
            return Objects.hash(response);
        }
    }

    @Override
    protected ResponseWrapper createTestInstance() {
        String id = randomAlphaOfLength(10);
        if (rarely()) {
            return new ResponseWrapper(xContentRegistry(), new GetWatchResponse(id));
        }
        long version = randomLongBetween(-1, Long.MAX_VALUE);
        WatchStatus status = randomWatchStatus();
        BytesReference source = simpleWatch();
        ResponseWrapper wrapper =
            new ResponseWrapper(xContentRegistry(), new GetWatchResponse(id, version, status, source, XContentType.JSON));
        return wrapper;
    }

    @Override
    protected ResponseWrapper doParseInstance(XContentParser parser) throws IOException {
        return new ResponseWrapper(xContentRegistry(), GetWatchResponse.fromXContent(parser));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    static String[] SHUFFLE_FIELDS_EXCEPTION = new String[] { "watch" };
    @Override
    protected String[] getShuffleFieldsExceptions() {
        return SHUFFLE_FIELDS_EXCEPTION;
    }

    @Override
    protected void assertEqualInstances(ResponseWrapper expectedInstance, ResponseWrapper newInstance) {
        if (newInstance.response.isFound() &&
                expectedInstance.response.getContentType() != newInstance.response.getContentType()) {
            /**
             * The {@link GetWatchResponse#getContentType()} depends on the content type that
             * was used to serialize the main object so we use the same content type than the
             * <code>expectedInstance</code> to translate the watch of the <code>newInstance</code>.
             */
            XContent from = XContentFactory.xContent(newInstance.response.getContentType());
            XContent to = XContentFactory.xContent(expectedInstance.response.getContentType());
            final BytesReference source;
            // It is safe to use EMPTY here because this never uses namedObject
            try (InputStream stream = newInstance.response.getSource().streamInput();
                 XContentParser parser = XContentFactory.xContent(from.type()).createParser(NamedXContentRegistry.EMPTY,
                     DeprecationHandler.THROW_UNSUPPORTED_OPERATION, stream)) {
                parser.nextToken();
                XContentBuilder builder = XContentFactory.contentBuilder(to.type());
                builder.copyCurrentStructure(parser);
                source = BytesReference.bytes(builder);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
            newInstance = new ResponseWrapper(xContentRegistry(),
                new GetWatchResponse(newInstance.response.getId(), newInstance.response.getVersion(),
                    newInstance.response.getStatus(), source, expectedInstance.response.getContentType()));
        }
        super.assertEqualInstances(expectedInstance, newInstance);
    }

    static XContentBuilder toXContentFragment(ActionStatus.Execution execution, XContentBuilder builder) throws IOException {
        builder.field("timestamp").value(DEFAULT_DATE_TIME_FORMATTER.printer().print(execution.timestamp()));
        builder.field("successful", execution.successful());
        if (execution.successful() == false) {
            builder.field("reason", execution.reason());
        }
        return builder;
    }

    static XContentBuilder toXContentFragment(ActionStatus.Throttle throttle, XContentBuilder builder) throws IOException {
        builder.field("timestamp").value(DEFAULT_DATE_TIME_FORMATTER.printer().print(throttle.timestamp()));
        builder.field("reason", throttle.reason());
        return builder;
    }

    static XContentBuilder toXContentFragment(ActionStatus.AckStatus status, XContentBuilder builder) throws IOException {
        builder.field("timestamp").value(DEFAULT_DATE_TIME_FORMATTER.printer().print(status.timestamp()));
        builder.field("state", status.state().name().toLowerCase(Locale.ROOT));
        return builder;
    }

    static XContentBuilder toXContentFragment(ActionStatus status, XContentBuilder builder) throws IOException {
        builder.startObject("ack");
        toXContentFragment(status.ackStatus(), builder);
        builder.endObject();
        if (status.lastExecution() != null) {
            builder.startObject("last_execution");
            toXContentFragment(status.lastExecution(), builder);
            builder.endObject();
        }
        if (status.lastSuccessfulExecution() != null) {
            builder.startObject("last_successful_execution");
            toXContentFragment(status.lastSuccessfulExecution(), builder);
            builder.endObject();
        }
        if (status.lastThrottle() != null) {
            builder.startObject("last_throttle");
            toXContentFragment(status.lastThrottle(), builder);
            builder.endObject();
        }
        return builder;
    }

    static XContentBuilder toXContent(WatchStatus status, XContentBuilder builder) throws IOException {
        builder.startObject("status");
        if (status.state() != null) {
            builder.startObject("state");
            builder.field("active", status.state().isActive());
            builder.field("timestamp", DEFAULT_DATE_TIME_FORMATTER.printer().print(status.state().getTimestamp()));
            builder.endObject();
        }

        if (status.lastChecked() != null) {
            builder.timeField("last_checked", status.lastChecked());
        }
        if (status.lastMetCondition() != null) {
            builder.timeField("last_met_condition",  status.lastMetCondition());
        }
        builder.startObject("actions");
        for (Map.Entry<String, ActionStatus> entry :status.getActions().entrySet()) {
            builder.startObject(entry.getKey());
            toXContentFragment(entry.getValue(), builder);
            builder.endObject();
        }
        builder.endObject();
        builder.field("execution_state", status.getExecutionState().id());
        builder.field("version", status.version());
        return builder.endObject();
    }

    static XContentParser parser(NamedXContentRegistry xContentRegistry,
                                    XContentType contentType, InputStream stream) throws IOException {
        return contentType.xContent().createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, stream);
    }

    static XContentBuilder toXContent(NamedXContentRegistry xContentRegistry,
                                        GetWatchResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("_id", response.getId());
        builder.field("found", response.isFound());
        if (response.isFound() == false) {
            return builder.endObject();
        }
        builder.field("_version", response.getVersion());
        toXContent(response.getStatus(), builder);
        builder.field("watch");
        try (InputStream stream = response.getSource().streamInput();
             XContentParser parser = parser(xContentRegistry, response.getContentType(), stream)) {
            parser.nextToken();
            builder.generator().copyCurrentStructure(parser);
        }
        return builder.endObject();
    }

    static ActionStatus.Execution randomExecution() {
        if (randomBoolean()) {
            return null;
        }
        boolean successful = randomBoolean();
        String reason = null;
        if (successful == false) {
            reason = randomAlphaOfLengthBetween(10, 20);

        }
        return new ActionStatus.Execution(DateTime.now(DateTimeZone.UTC), successful, reason);
    }

    static WatchStatus randomWatchStatus() {
        long version = randomLongBetween(-1, Long.MAX_VALUE);
        WatchStatus.State state = rarely() ? null : new WatchStatus.State(randomBoolean(), DateTime.now(DateTimeZone.UTC));
        ExecutionState executionState = randomFrom(ExecutionState.values());
        DateTime lastChecked = rarely() ? null : DateTime.now(DateTimeZone.UTC);
        DateTime lastMetCondition = rarely() ? null : DateTime.now(DateTimeZone.UTC);
        int size = randomIntBetween(0, 5);
        Map<String, ActionStatus> actionMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            ActionStatus.AckStatus ack = new ActionStatus.AckStatus(DateTime.now(DateTimeZone.UTC),
                randomFrom(ActionStatus.AckStatus.State.values()));
            ActionStatus actionStatus = new ActionStatus(ack, randomExecution(), randomExecution(), null);
            actionMap.put(randomAlphaOfLength(10), actionStatus);
        }
        return new WatchStatus(version, state, executionState, lastChecked, lastMetCondition, actionMap);
    }

    static BytesReference simpleWatch() {
        XContentBuilder builder = null;
        try {
            builder = XContentBuilder.builder(XContentType.JSON.xContent());
            builder.startObject()
                .startObject("trigger")
                    .startObject("schedule")
                        .field("interval", "10h")
                    .endObject()
                .endObject()
               .startObject("input")
                    .startObject("none")
                    .endObject()
                .endObject()
                .startObject("actions")
                    .startObject("logme")
                        .field("text", "{{ctx.payload}}")
                    .endObject()
                .endObject().endObject();
            return BytesReference.bytes(builder);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
