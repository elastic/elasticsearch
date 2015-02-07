/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.alerts.actions.email.EmailAction;
import org.elasticsearch.alerts.actions.email.EmailSettingsService;
import org.elasticsearch.alerts.actions.index.IndexAction;
import org.elasticsearch.alerts.actions.webhook.HttpClient;
import org.elasticsearch.alerts.actions.webhook.WebhookAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.multibindings.MapBinder;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class ActionModule extends AbstractModule {

    private final Map<String, Class<? extends Action.Parser>> parsers = new HashMap<>();

    public void registerAction(String type, Class<? extends Action.Parser> parserType) {
        parsers.put(type, parserType);
    }

    @Override
    protected void configure() {

        MapBinder<String, Action.Parser> parsersBinder = MapBinder.newMapBinder(binder(), String.class, Action.Parser.class);
        bind(EmailAction.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(EmailAction.TYPE).to(EmailAction.Parser.class);

        bind(WebhookAction.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(WebhookAction.TYPE).to(WebhookAction.Parser.class);

        bind(IndexAction.Parser.class).asEagerSingleton();
        parsersBinder.addBinding(IndexAction.TYPE).to(IndexAction.Parser.class);


        for (Map.Entry<String, Class<? extends Action.Parser>> entry : parsers.entrySet()) {
            bind(entry.getValue()).asEagerSingleton();
            parsersBinder.addBinding(entry.getKey()).to(entry.getValue());
        }

        bind(ActionRegistry.class).asEagerSingleton();
        bind(HttpClient.class).asEagerSingleton();
        bind(EmailSettingsService.class).asEagerSingleton();
    }


}
