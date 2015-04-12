/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.watcher.actions.email.EmailAction;
import org.elasticsearch.watcher.actions.email.service.EmailTemplate;
import org.elasticsearch.watcher.actions.index.IndexAction;
import org.elasticsearch.watcher.actions.logging.LoggingAction;
import org.elasticsearch.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.watcher.support.http.HttpRequestTemplate;
import org.elasticsearch.watcher.support.template.Template;

/**
 *
 */
public final class ActionBuilders {

    private ActionBuilders() {
    }

    public static EmailAction.SourceBuilder emailAction(EmailTemplate.Builder email) {
        return emailAction(email.build());
    }

    public static EmailAction.SourceBuilder emailAction(EmailTemplate email) {
        return new EmailAction.SourceBuilder(email);
    }

    public static IndexAction.SourceBuilder indexAction(String index, String type) {
        return new IndexAction.SourceBuilder(index, type);
    }

    public static WebhookAction.SourceBuilder webhookAction(HttpRequestTemplate.Builder httpRequest) {
        return new WebhookAction.SourceBuilder(httpRequest.build());
    }

    public static WebhookAction.SourceBuilder webhookAction(HttpRequestTemplate httpRequest) {
        return new WebhookAction.SourceBuilder(httpRequest);
    }

    public static LoggingAction.SourceBuilder loggingAction(String text) {
        return loggingAction(new Template(text));
    }

    public static LoggingAction.SourceBuilder loggingAction(Template text) {
        return new LoggingAction.SourceBuilder(text);
    }

}
