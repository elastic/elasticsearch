/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.rest.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.xpack.core.watcher.client.WatcherClient;
import org.elasticsearch.xpack.core.watcher.transport.actions.croneval.CronEvaluationRequest;
import org.elasticsearch.xpack.core.watcher.transport.actions.croneval.CronEvaluationResponse;
import org.elasticsearch.xpack.watcher.rest.WatcherRestHandler;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestCronEvalAction extends WatcherRestHandler {

    private static final ParseField EXPRESSIONS = new ParseField("timestamps");
    private static final ObjectParser<CronEvaluationRequest, Void> PARSER =
         new ObjectParser<>("croneval", CronEvaluationRequest::new);

    static {
        PARSER.declareString(CronEvaluationRequest::setExpression, new ParseField("expression"));
        PARSER.declareInt(CronEvaluationRequest::setCount, new ParseField("count"));
    }

    public RestCronEvalAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(GET, URI_BASE + "/_croneval", this);
        // allow post, so clients having issues with GET and bodies can use this API as well
        controller.registerHandler(POST, URI_BASE + "/_croneval", this);
    }

    @Override
    public String getName() {
        return "xpack_watcher_cron_eval_action";
    }

    @Override
    protected RestChannelConsumer doPrepareRequest(RestRequest request, WatcherClient client) throws IOException {
        CronEvaluationRequest cronEvaluationRequest = PARSER.parse(request.contentParser(), null);
        return channel -> client.cronEvaluation(cronEvaluationRequest, new RestBuilderListener<CronEvaluationResponse>(channel) {
            @Override
            public RestResponse buildResponse(CronEvaluationResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                builder.field(EXPRESSIONS.getPreferredName(), response.getTimestamps());
                builder.endObject();
                return new BytesRestResponse(RestStatus.OK, builder);
            }
        });
    }
}
