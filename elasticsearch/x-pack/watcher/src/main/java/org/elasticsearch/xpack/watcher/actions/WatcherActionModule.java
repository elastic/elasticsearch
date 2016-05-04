/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;
import org.elasticsearch.xpack.watcher.actions.email.EmailAction;
import org.elasticsearch.xpack.watcher.actions.email.EmailActionFactory;
import org.elasticsearch.xpack.watcher.actions.hipchat.HipChatAction;
import org.elasticsearch.xpack.watcher.actions.hipchat.HipChatActionFactory;
import org.elasticsearch.xpack.watcher.actions.index.IndexAction;
import org.elasticsearch.xpack.watcher.actions.index.IndexActionFactory;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingAction;
import org.elasticsearch.xpack.watcher.actions.logging.LoggingActionFactory;
import org.elasticsearch.xpack.watcher.actions.pagerduty.PagerDutyAction;
import org.elasticsearch.xpack.watcher.actions.pagerduty.PagerDutyActionFactory;
import org.elasticsearch.xpack.watcher.actions.slack.SlackAction;
import org.elasticsearch.xpack.watcher.actions.slack.SlackActionFactory;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookAction;
import org.elasticsearch.xpack.watcher.actions.webhook.WebhookActionFactory;

import java.util.HashMap;
import java.util.Map;

public class WatcherActionModule extends AbstractModule {

    private final Map<String, Class<? extends ActionFactory>> parsers = new HashMap<>();

    public WatcherActionModule() {
        registerAction(EmailAction.TYPE, EmailActionFactory.class);
        registerAction(WebhookAction.TYPE, WebhookActionFactory.class);
        registerAction(IndexAction.TYPE, IndexActionFactory.class);
        registerAction(LoggingAction.TYPE, LoggingActionFactory.class);
        registerAction(HipChatAction.TYPE, HipChatActionFactory.class);
        registerAction(SlackAction.TYPE, SlackActionFactory.class);
        registerAction(PagerDutyAction.TYPE, PagerDutyActionFactory.class);
    }

    public void registerAction(String type, Class<? extends ActionFactory> parserType) {
        parsers.put(type, parserType);
    }

    @Override
    protected void configure() {
        MapBinder<String, ActionFactory> parsersBinder = MapBinder.newMapBinder(binder(), String.class, ActionFactory.class);
        for (Map.Entry<String, Class<? extends ActionFactory>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            parsersBinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(ActionRegistry.class).asEagerSingleton();

    }
}
