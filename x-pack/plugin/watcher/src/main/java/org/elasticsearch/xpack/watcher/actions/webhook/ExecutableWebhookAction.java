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
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.WebhookService;

public class ExecutableWebhookAction extends ExecutableAction<WebhookAction> {

    private final WebhookService webhookService;
    private final TextTemplateEngine templateEngine;

    public ExecutableWebhookAction(WebhookAction action, Logger logger, WebhookService webhookService, TextTemplateEngine templateEngine) {
        super(action, logger);
        this.webhookService = webhookService;
        this.templateEngine = templateEngine;
    }

    @Override
    public Action.Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        return webhookService.execute(actionId, action, templateEngine, ctx, payload);
    }
}
