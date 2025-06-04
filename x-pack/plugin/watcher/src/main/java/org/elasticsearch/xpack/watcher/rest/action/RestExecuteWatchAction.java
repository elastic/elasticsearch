/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFilter;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchAction;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.core.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.core.watcher.watch.WatchField;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xpack.watcher.rest.action.RestExecuteWatchAction.Field.IGNORE_CONDITION;
import static org.elasticsearch.xpack.watcher.rest.action.RestExecuteWatchAction.Field.RECORD_EXECUTION;

public class RestExecuteWatchAction extends BaseRestHandler implements RestRequestFilter {

    private static final List<String> RESERVED_FIELD_NAMES = asList(
        WatchField.TRIGGER.getPreferredName(),
        WatchField.INPUT.getPreferredName(),
        WatchField.CONDITION.getPreferredName(),
        WatchField.ACTIONS.getPreferredName(),
        WatchField.TRANSFORM.getPreferredName(),
        WatchField.THROTTLE_PERIOD.getPreferredName(),
        WatchField.THROTTLE_PERIOD_HUMAN.getPreferredName(),
        WatchField.METADATA.getPreferredName(),
        WatchField.STATUS.getPreferredName()
    );

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_watcher/watch/{id}/_execute"),
            new Route(PUT, "/_watcher/watch/{id}/_execute"),
            new Route(POST, "/_watcher/watch/_execute"),
            new Route(PUT, "/_watcher/watch/_execute")
        );
    }

    @Override
    public String getName() {
        return "watcher_execute_watch";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, NodeClient client) throws IOException {
        ExecuteWatchRequest executeWatchRequest = parseRequest(request, client);

        return channel -> client.execute(ExecuteWatchAction.INSTANCE, executeWatchRequest, new RestBuilderListener<>(channel) {
            @Override
            public RestResponse buildResponse(ExecuteWatchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Field.ID.getPreferredName(), response.getRecordId());
                builder.field(Field.WATCH_RECORD.getPreferredName(), response.getRecordSource(), request);
                builder.endObject();
                return new RestResponse(RestStatus.OK, builder);
            }
        });
    }

    // This tightly binds the REST API to the java API. pkg private for testing
    static ExecuteWatchRequest parseRequest(RestRequest request, NodeClient client) throws IOException {
        ExecuteWatchRequestBuilder builder = new ExecuteWatchRequestBuilder(client);
        builder.setId(request.param("id"));
        builder.setDebug(WatcherParams.debug(request));

        if (request.hasContent() == false) {
            return builder.request();
        }

        builder.setRecordExecution(request.paramAsBoolean(RECORD_EXECUTION.getPreferredName(), builder.request().isRecordExecution()));
        builder.setIgnoreCondition(request.paramAsBoolean(IGNORE_CONDITION.getPreferredName(), builder.request().isIgnoreCondition()));

        try (XContentParser parser = request.contentParser()) {
            parser.nextToken();

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (IGNORE_CONDITION.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.setIgnoreCondition(parser.booleanValue());
                    } else if (RECORD_EXECUTION.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.setRecordExecution(parser.booleanValue());
                    } else {
                        throw new ElasticsearchParseException(
                            "could not parse watch execution request. unexpected boolean field [{}]",
                            currentFieldName
                        );
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (Field.ALTERNATIVE_INPUT.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.setAlternativeInput(parser.map());
                    } else if (Field.TRIGGER_DATA.match(currentFieldName, parser.getDeprecationHandler())) {
                        builder.setTriggerData(parser.map());
                    } else if (Field.WATCH.match(currentFieldName, parser.getDeprecationHandler())) {
                        try (XContentBuilder watcherSource = XContentBuilder.builder(parser.contentType().xContent())) {
                            watcherSource.generator().copyCurrentStructure(parser);
                            builder.setWatchSource(BytesReference.bytes(watcherSource), parser.contentType());
                        }
                    } else if (Field.ACTION_MODES.match(currentFieldName, parser.getDeprecationHandler())) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.VALUE_STRING) {
                                try {
                                    ActionExecutionMode mode = ActionExecutionMode.resolve(parser.textOrNull());
                                    builder.setActionMode(currentFieldName, mode);
                                } catch (IllegalArgumentException iae) {
                                    throw new ElasticsearchParseException("could not parse watch execution request", iae);
                                }
                            } else {
                                throw new ElasticsearchParseException(
                                    "could not parse watch execution request. unexpected array field [{}]",
                                    currentFieldName
                                );
                            }
                        }
                    } else {
                        if (RESERVED_FIELD_NAMES.contains(currentFieldName)) {
                            throw new ElasticsearchParseException(
                                "please wrap watch including field [{}] inside a \"watch\" field",
                                currentFieldName
                            );
                        } else {
                            throw new ElasticsearchParseException(
                                "could not parse watch execution request. unexpected object field [{}]",
                                currentFieldName
                            );
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse watch execution request. unexpected token [{}]", token);
                }
            }
        }

        return builder.request();
    }

    private static final Set<String> FILTERED_FIELDS = Set.of(
        "watch.input.http.request.auth.basic.password",
        "watch.input.chain.inputs.*.http.request.auth.basic.password",
        "watch.actions.*.email.attachments.*.reporting.auth.basic.password",
        "watch.actions.*.webhook.auth.basic.password"
    );

    @Override
    public Set<String> getFilteredFields() {
        return FILTERED_FIELDS;
    }

    interface Field {
        ParseField ID = new ParseField("_id");
        ParseField WATCH_RECORD = new ParseField("watch_record");

        ParseField RECORD_EXECUTION = new ParseField("record_execution");
        ParseField ACTION_MODES = new ParseField("action_modes");
        ParseField ALTERNATIVE_INPUT = new ParseField("alternative_input");
        ParseField IGNORE_CONDITION = new ParseField("ignore_condition");
        ParseField TRIGGER_DATA = new ParseField("trigger_data");
        ParseField WATCH = new ParseField("watch");
    }
}
