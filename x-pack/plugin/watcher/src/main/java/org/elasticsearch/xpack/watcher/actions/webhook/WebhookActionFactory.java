/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.webhook;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.WebhookService;

import java.io.IOException;

public class WebhookActionFactory extends ActionFactory {

    private final TextTemplateEngine templateEngine;
    private final WebhookService webhookService;

    public WebhookActionFactory(WebhookService webhookService, TextTemplateEngine templateEngine) {
        super(LogManager.getLogger(ExecutableWebhookAction.class));
        this.templateEngine = templateEngine;
        this.webhookService = webhookService;
    }

    @Override
    public ExecutableWebhookAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        return new ExecutableWebhookAction(WebhookAction.parse(watchId, actionId, parser), actionLogger, webhookService, templateEngine);

    }
}
