/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.webhook;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.util.Map;

public class ExecutableWebhookAction extends ExecutableAction<WebhookAction> {

    private final HttpClient httpClient;
    private final TextTemplateEngine templateEngine;

    public ExecutableWebhookAction(WebhookAction action, Logger logger, HttpClient httpClient, TextTemplateEngine templateEngine) {
        super(action, logger);
        this.httpClient = httpClient;
        this.templateEngine = templateEngine;
    }

    @Override
    public Action.Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        Map<String, Object> model = Variables.createCtxParamsMap(ctx, payload);

        HttpRequest request = action.requestTemplate.render(templateEngine, model);

        if (ctx.simulateAction(actionId)) {
            return new WebhookAction.Result.Simulated(request);
        }

        HttpResponse response = httpClient.execute(request);

        if (response.status() >= 400) {
            return new WebhookAction.Result.Failure(request, response);
        } else {
            return new WebhookAction.Result.Success(request, response);
        }
    }
}
