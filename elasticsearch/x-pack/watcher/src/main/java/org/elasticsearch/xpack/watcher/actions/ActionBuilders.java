/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.xpack.watcher.actions.email.EmailAction;
import org.elasticsearch.xpack.watcher.actions.hipchat.HipChatAction;
import org.elasticsearch.xpack.watcher.actions.index.IndexAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.actions.pagerduty.PagerDutyAction;
import org.elasticsearch.xpack.notification.email.EmailTemplate;
import org.elasticsearch.xpack.notification.pagerduty.IncidentEvent;
import org.elasticsearch.xpack.watcher.actions.slack.SlackAction;
import org.elasticsearch.xpack.notification.slack.message.SlackMessage;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.common.http.HttpRequestTemplate;
import org.elasticsearch.xpack.common.text.TextTemplate;

/**
 *
 */
public final class ActionBuilders {

    private ActionBuilders() {
    }

    public static EmailAction.Builder emailAction(EmailTemplate.Builder email) {
        return emailAction(email.build());
    }

    public static EmailAction.Builder emailAction(EmailTemplate email) {
        return EmailAction.builder(email);
    }

    public static IndexAction.Builder indexAction(String index, String type) {
        return IndexAction.builder(index, type);
    }

    public static WebhookAction.Builder webhookAction(HttpRequestTemplate.Builder httpRequest) {
        return webhookAction(httpRequest.build());
    }

    public static WebhookAction.Builder webhookAction(HttpRequestTemplate httpRequest) {
        return WebhookAction.builder(httpRequest);
    }

    public static LoggingAction.Builder loggingAction(String text) {
        return loggingAction(TextTemplate.inline(text));
    }

    public static LoggingAction.Builder loggingAction(TextTemplate.Builder text) {
        return loggingAction(text.build());
    }

    public static LoggingAction.Builder loggingAction(TextTemplate text) {
        return LoggingAction.builder(text);
    }

    public static HipChatAction.Builder hipchatAction(String message) {
        return hipchatAction(TextTemplate.inline(message));
    }

    public static HipChatAction.Builder hipchatAction(String account, String body) {
        return hipchatAction(account, TextTemplate.inline(body));
    }

    public static HipChatAction.Builder hipchatAction(TextTemplate.Builder body) {
        return hipchatAction(body.build());
    }

    public static HipChatAction.Builder hipchatAction(String account, TextTemplate.Builder body) {
        return hipchatAction(account, body.build());
    }

    public static HipChatAction.Builder hipchatAction(TextTemplate body) {
        return hipchatAction(null, body);
    }

    public static HipChatAction.Builder hipchatAction(String account, TextTemplate body) {
        return HipChatAction.builder(account, body);
    }

    public static SlackAction.Builder slackAction(String account, SlackMessage.Template.Builder message) {
        return slackAction(account, message.build());
    }

    public static SlackAction.Builder slackAction(String account, SlackMessage.Template message) {
        return SlackAction.builder(account, message);
    }

    public static PagerDutyAction.Builder triggerPagerDutyAction(String account, String description) {
        return pagerDutyAction(IncidentEvent.templateBuilder(description).setAccount(account));
    }

    public static PagerDutyAction.Builder pagerDutyAction(IncidentEvent.Template.Builder event) {
        return PagerDutyAction.builder(event.build());
    }

    public static PagerDutyAction.Builder pagerDutyAction(IncidentEvent.Template event) {
        return PagerDutyAction.builder(event);
    }
}
