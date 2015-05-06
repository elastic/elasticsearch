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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.*;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.watcher.rest.WatcherRestHandler;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.watcher.trigger.TriggerEvent;
import org.elasticsearch.watcher.trigger.TriggerService;

import java.io.IOException;

/**
 */
public class RestExecuteWatchAction extends WatcherRestHandler {

    static ParseField RECORD_EXECUTION_FIELD = new ParseField("record_execution");
    static ParseField SIMULATED_ACTIONS_FIELD = new ParseField("simulated_actions");
    static ParseField ALTERNATIVE_INPUT_FIELD = new ParseField("alternative_input");
    static ParseField IGNORE_CONDITION_FIELD = new ParseField("ignore_condition");
    static ParseField IGNORE_THROTTLE_FIELD = new ParseField("ignore_throttle");
    static ParseField TRIGGER_EVENT_FIELD = new ParseField("trigger_event");

    final TriggerService triggerService;

    @Inject
    public RestExecuteWatchAction(Settings settings, RestController controller, Client client, TriggerService triggerService) {
        super(settings, controller, client);
        controller.registerHandler(RestRequest.Method.POST, URI_BASE + "/watch/{id}/_execute", this);
        controller.registerHandler(RestRequest.Method.PUT, URI_BASE + "/watch/{id}/_execute", this);
        this.triggerService = triggerService;
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, WatcherClient client) throws Exception {
        ExecuteWatchRequest executeWatchRequest = parseRequest(request, client);

        client.executeWatch(executeWatchRequest, new RestBuilderListener<ExecuteWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(ExecuteWatchResponse response, XContentBuilder builder) throws Exception {
                builder.value(response.getSource());
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    //This tightly binds the REST API to the java API
    private ExecuteWatchRequest parseRequest(RestRequest request, WatcherClient client) throws IOException {
        String watchId = request.param("id");

        ExecuteWatchRequestBuilder executeWatchRequestBuilder = client.prepareExecuteWatch(watchId);
        TriggerEvent triggerEvent = null;




        if (request.content() != null && request.content().length() != 0) {
            XContentParser parser = XContentHelper.createParser(request.content());
            parser.nextToken();

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            for (; token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
                switch (token) {
                    case FIELD_NAME:
                        currentFieldName = parser.currentName();
                        break;
                    case VALUE_BOOLEAN:
                        if (IGNORE_CONDITION_FIELD.match(currentFieldName)) {
                            executeWatchRequestBuilder.setIgnoreCondition(parser.booleanValue());
                        } else if (IGNORE_THROTTLE_FIELD.match(currentFieldName)) {
                            executeWatchRequestBuilder.setIgnoreThrottle(parser.booleanValue());
                        } else if (RECORD_EXECUTION_FIELD.match(currentFieldName)) {
                            executeWatchRequestBuilder.setRecordExecution(parser.booleanValue());
                        } else {
                            throw new ParseException("invalid watch execution request, unexpected boolean value field [" + currentFieldName + "]");
                        }
                        break;
                    case START_OBJECT:
                        if (ALTERNATIVE_INPUT_FIELD.match(currentFieldName)) {
                            executeWatchRequestBuilder.setAlternativeInput(parser.map());
                        } else if (TRIGGER_EVENT_FIELD.match(currentFieldName)) {
                            triggerEvent = triggerService.parseTriggerEvent(watchId, watchId, parser);
                        } else {
                            throw new ParseException("invalid watch execution request, unexpected object value field [" + currentFieldName + "]");
                        }
                        break;
                    case START_ARRAY:
                        if (SIMULATED_ACTIONS_FIELD.match(currentFieldName)) {
                            for (XContentParser.Token arrayToken = parser.nextToken(); arrayToken != XContentParser.Token.END_ARRAY; arrayToken = parser.nextToken()) {
                                if (arrayToken == XContentParser.Token.VALUE_STRING) {
                                    executeWatchRequestBuilder.addSimulatedActions(parser.text());
                                }
                            }
                        } else {
                            throw new ParseException("invalid watch execution request, unexpected array value field [" + currentFieldName + "]");
                        }
                        break;
                    case VALUE_STRING:
                        if (SIMULATED_ACTIONS_FIELD.match(currentFieldName)) {
                            if (parser.text().equals("_all")) {
                                executeWatchRequestBuilder.addSimulatedActions("_all");
                            } else {
                                throw new ParseException("invalid watch execution request, unexpected string value [" + parser.text() + "] for field [" + SIMULATED_ACTIONS_FIELD.getPreferredName() + "]");
                            }
                        } else {
                            throw new ParseException("invalid watch execution request, unexpected string value field [" + currentFieldName + "]");
                        }
                        break;
                    default:
                        throw new ParseException("invalid watch execution request, unexpected token field [" + token + "]");
                }
            }
        }
        if (triggerEvent == null) {
            throw new WatcherException("[{}] is a required field.",TRIGGER_EVENT_FIELD.getPreferredName());
        }

        return executeWatchRequestBuilder.request();
    }

    public static class ParseException extends WatcherException {
        public ParseException(String msg) {
            super(msg);
        }

        public ParseException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }
}
