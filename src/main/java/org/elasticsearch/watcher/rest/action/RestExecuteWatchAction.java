/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.execution.ActionExecutionMode;
import org.elasticsearch.watcher.rest.WatcherRestHandler;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.trigger.TriggerService;

import java.io.IOException;

/**
 */
public class RestExecuteWatchAction extends WatcherRestHandler {

    final TriggerService triggerService;

    @Inject
    public RestExecuteWatchAction(Settings settings, RestController controller, Client client, TriggerService triggerService) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/_execute", this);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/_execute", this);
        this.triggerService = triggerService;
    }

    @Override
    protected void handleRequest(final RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        ExecuteWatchRequest executeWatchRequest = parseRequest(request, client);

        client.executeWatch(executeWatchRequest, new RestBuilderListener<ExecuteWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(ExecuteWatchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Field.ID.getPreferredName(), response.getRecordId());
                builder.field(Field.WATCH_RECORD.getPreferredName(), response.getRecordSource(), ToXContent.EMPTY_PARAMS);
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    //This tightly binds the REST API to the java API
    private ExecuteWatchRequest parseRequest(RestRequest request, WatcherClient client) throws IOException {
        String watchId = request.param("id");
        ExecuteWatchRequestBuilder builder = client.prepareExecuteWatch(watchId);

        if (request.content() == null || request.content().length() == 0) {
            return builder.request();
        }

        XContentParser parser = XContentHelper.createParser(request.content());
        parser.nextToken();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (Field.IGNORE_CONDITION.match(currentFieldName)) {
                    builder.setIgnoreCondition(parser.booleanValue());
                } else if (Field.RECORD_EXECUTION.match(currentFieldName)) {
                    builder.setRecordExecution(parser.booleanValue());
                } else {
                    throw new ParseException("could not parse watch execution request for [{}]. unexpected boolean field [{}]", watchId, currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Field.ALTERNATIVE_INPUT.match(currentFieldName)) {
                    builder.setAlternativeInput(parser.map());
                } else if (Field.TRIGGER_DATA.match(currentFieldName)) {
                    builder.setTriggerData(parser.map());
                } else if (Field.ACTION_MODES.match(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            try {
                                ActionExecutionMode mode = ActionExecutionMode.resolve(parser.textOrNull());
                                builder.setActionMode(currentFieldName, mode);
                            } catch (WatcherException we) {
                                throw new ParseException("could not parse watch execution request for [{}].", watchId, we);
                            }
                        } else {
                            throw new ParseException("could not parse watch execution request for [{}]. unexpected array field [{}]", watchId, currentFieldName);
                        }
                    }
                } else {
                    throw new ParseException("could not parse watch execution request for [{}]. unexpected object field [{}]", watchId, currentFieldName);
                }
            } else {
                throw new ParseException("could not parse watch execution request for [{}]. unexpected token [{}]", watchId, token);
            }
        }

        return builder.request();
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg, Object... args) {
            super(msg, args);
        }

        public ParseException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }

    interface Field {
        ParseField ID = new ParseField("_id");
        ParseField WATCH_RECORD = new ParseField("watch_record");

        ParseField RECORD_EXECUTION = new ParseField("record_execution");
        ParseField ACTION_MODES = new ParseField("action_modes");
        ParseField ALTERNATIVE_INPUT = new ParseField("alternative_input");
        ParseField IGNORE_CONDITION = new ParseField("ignore_condition");
        ParseField TRIGGER_DATA = new ParseField("trigger_data");
    }
}
