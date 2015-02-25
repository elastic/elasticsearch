/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.alerts.actions.email.EmailAction;
import org.elasticsearch.alerts.actions.index.IndexAction;
import org.elasticsearch.alerts.actions.webhook.WebhookAction;
import org.elasticsearch.alerts.support.Script;

/**
 *
 */
public final class ActionBuilders {

    private ActionBuilders() {
    }

    public static EmailAction.SourceBuilder emailAction() {
        return new EmailAction.SourceBuilder();
    }

    public static IndexAction.SourceBuilder indexAction(String index, String type) {
        return new IndexAction.SourceBuilder(index, type);
    }

    public static WebhookAction.SourceBuilder webhookAction(String url) {
        return new WebhookAction.SourceBuilder(url);
    }

    public static WebhookAction.SourceBuilder webhookAction(Script url) {
        return new WebhookAction.SourceBuilder(url);
    }
}
