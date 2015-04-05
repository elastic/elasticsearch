/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions;

import org.elasticsearch.watcher.actions.email.EmailAction;
import org.elasticsearch.watcher.actions.index.IndexAction;
import org.elasticsearch.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.watcher.support.http.TemplatedHttpRequest;

/**
 *
 */
public final class ActionBuilders {

    private ActionBuilders() {
    }

    public static EmailAction.SourceBuilder emailAction(String id) {
        return new EmailAction.SourceBuilder(id);
    }

    public static IndexAction.SourceBuilder indexAction(String id, String index, String type) {
        return new IndexAction.SourceBuilder(id, index, type);
    }

    public static WebhookAction.SourceBuilder webhookAction(String id, TemplatedHttpRequest.SourceBuilder httpRequest) {
        return new WebhookAction.SourceBuilder(id, httpRequest);
    }

}
