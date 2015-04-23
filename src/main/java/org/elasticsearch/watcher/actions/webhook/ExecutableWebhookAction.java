/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.webhook;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.actions.ExecutableAction;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.http.HttpClient;
import org.elasticsearch.watcher.support.http.HttpRequest;
import org.elasticsearch.watcher.support.http.HttpResponse;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Map;

/**
 */
public class ExecutableWebhookAction extends ExecutableAction<WebhookAction, WebhookAction.Result> {

    private final HttpClient httpClient;
    private final TemplateEngine templateEngine;

    public ExecutableWebhookAction(WebhookAction action, ESLogger logger, HttpClient httpClient, TemplateEngine templateEngine) {
        super(action, logger);
        this.httpClient = httpClient;
        this.templateEngine = templateEngine;
    }

    @Override
    protected WebhookAction.Result doExecute(String actionId, WatchExecutionContext ctx, Payload payload) throws IOException {
        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        HttpRequest request = action.requestTemplate.render(templateEngine, model);

        if (ctx.simulateAction(actionId)) {
            return new WebhookAction.Result.Simulated(request);
        }

        HttpResponse response = httpClient.execute(request);

        int status = response.status();
        if (status >= 300) {
            logger.warn("received http status [{}] when connecting to watch action [{}/{}/{}]", status, ctx.watch().id(), type(), actionId);
        }
        return new WebhookAction.Result.Executed(request, response);
    }

    @Override
    protected WebhookAction.Result failure(String reason) {
        return new WebhookAction.Result.Failure(reason);
    }
}
