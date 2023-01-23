/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.notification;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.common.http.HttpClient;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * The WebhookService class handles executing webhook requests for Watcher actions. These can be
 * regular "webhook" actions as well as parts of an "email" action with attachments that make HTTP
 * requests.
 */
public class WebhookService extends NotificationService<WebhookService.WebhookAccount> {

    private final HttpClient httpClient;

    public WebhookService(Settings settings, HttpClient httpClient, ClusterSettings clusterSettings) {
        super("webhook", settings, clusterSettings, List.of(), getSecureSettings());
        this.httpClient = httpClient;
        // do an initial load
        reload(settings);
    }

    private static List<Setting<?>> getSecureSettings() {
        return List.of();
    }

    @Override
    protected WebhookAccount createAccount(String name, Settings accountSettings) {
        return new WebhookAccount();
    }

    public Action.Result execute(
        String actionId,
        WebhookAction action,
        TextTemplateEngine templateEngine,
        WatchExecutionContext ctx,
        Payload payload
    ) throws IOException {
        Map<String, Object> model = Variables.createCtxParamsMap(ctx, payload);

        HttpRequest request = action.getRequest().render(templateEngine, model);

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

    public static final class WebhookAccount {}
}
