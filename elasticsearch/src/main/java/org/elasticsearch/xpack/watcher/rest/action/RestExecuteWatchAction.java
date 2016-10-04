/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.watcher.client.WatcherClient;
import org.elasticsearch.xpack.watcher.execution.ActionExecutionMode;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;
import org.elasticsearch.xpack.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchRequest;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchRequestBuilder;
import org.elasticsearch.xpack.watcher.transport.actions.execute.ExecuteWatchResponse;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;
import static org.elasticsearch.xpack.watcher.rest.action.RestExecuteWatchAction.Field.IGNORE_CONDITION;
import static org.elasticsearch.xpack.watcher.rest.action.RestExecuteWatchAction.Field.RECORD_EXECUTION;

/**
 */
public class RestExecuteWatchAction extends WatcherRestHandler {

    final TriggerService triggerService;

    @Inject
    public RestExecuteWatchAction(Settings settings, RestController controller, TriggerService triggerService) {
        super(settings);
        this.triggerService = triggerService;

        // @deprecated Remove deprecations in 6.0
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/watch/{id}/_execute", this,
                                                 POST, "/_watcher/watch/{id}/_execute", deprecationLogger);
        controller.registerWithDeprecatedHandler(PUT, URI_BASE + "/watch/{id}/_execute", this,
                                                 PUT, "/_watcher/watch/{id}/_execute", deprecationLogger);
        controller.registerWithDeprecatedHandler(POST, URI_BASE + "/watch/_execute", this,
                                                 POST, "/_watcher/watch/_execute", deprecationLogger);
        controller.registerWithDeprecatedHandler(PUT, URI_BASE + "/watch/_execute", this,
                                                 PUT, "/_watcher/watch/_execute", deprecationLogger);
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(final RestRequest request, WatcherClient client) throws IOException {
        ExecuteWatchRequest executeWatchRequest = parseRequest(request, client);

        return channel -> client.executeWatch(executeWatchRequest, new RestBuilderListener<ExecuteWatchResponse>(channel) {
            @Override
            public RestResponse buildResponse(ExecuteWatchResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(Field.ID.getPreferredName(), response.getRecordId());
                builder.field(Field.WATCH_RECORD.getPreferredName(), response.getRecordSource(), request);
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }

    //This tightly binds the REST API to the java API
    private ExecuteWatchRequest parseRequest(RestRequest request, WatcherClient client) throws IOException {
        ExecuteWatchRequestBuilder builder = client.prepareExecuteWatch();
        builder.setId(request.param("id"));
        builder.setDebug(WatcherParams.debug(request));

        if (request.content() == null || request.content().length() == 0) {
            return builder.request();
        }

        builder.setRecordExecution(request.paramAsBoolean(RECORD_EXECUTION.getPreferredName(), builder.request().isRecordExecution()));
        builder.setIgnoreCondition(request.paramAsBoolean(IGNORE_CONDITION.getPreferredName(), builder.request().isIgnoreCondition()));

        XContentParser parser = XContentHelper.createParser(request.content());
        parser.nextToken();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (ParseFieldMatcher.STRICT.match(currentFieldName, IGNORE_CONDITION)) {
                    builder.setIgnoreCondition(parser.booleanValue());
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, RECORD_EXECUTION)) {
                    builder.setRecordExecution(parser.booleanValue());
                } else {
                    throw new ElasticsearchParseException("could not parse watch execution request. unexpected boolean field [{}]",
                            currentFieldName);
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.ALTERNATIVE_INPUT)) {
                    builder.setAlternativeInput(parser.map());
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.TRIGGER_DATA)) {
                    builder.setTriggerData(parser.map());
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.WATCH)) {
                    XContentBuilder watcherSource = XContentBuilder.builder(parser.contentType().xContent());
                    XContentHelper.copyCurrentStructure(watcherSource.generator(), parser);
                    builder.setWatchSource(watcherSource.bytes());
                } else if (ParseFieldMatcher.STRICT.match(currentFieldName, Field.ACTION_MODES)) {
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
                            throw new ElasticsearchParseException("could not parse watch execution request. unexpected array field [{}]",
                                    currentFieldName);
                        }
                    }
                } else {
                    throw new ElasticsearchParseException("could not parse watch execution request. unexpected object field [{}]",
                            currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("could not parse watch execution request. unexpected token [{}]", token);
            }
        }

        return builder.request();
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
