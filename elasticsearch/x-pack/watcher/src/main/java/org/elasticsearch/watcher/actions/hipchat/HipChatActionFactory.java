/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.hipchat;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.notification.hipchat.HipChatAccount;
import org.elasticsearch.xpack.notification.hipchat.HipChatService;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;

import java.io.IOException;

/**
 *
 */
public class HipChatActionFactory extends ActionFactory<HipChatAction, ExecutableHipChatAction> {

    private final TextTemplateEngine templateEngine;
    private final HipChatService hipchatService;

    @Inject
    public HipChatActionFactory(Settings settings, TextTemplateEngine templateEngine, HipChatService hipchatService) {
        super(Loggers.getLogger(ExecutableHipChatAction.class, settings));
        this.templateEngine = templateEngine;
        this.hipchatService = hipchatService;
    }

    @Override
    public String type() {
        return HipChatAction.TYPE;
    }

    @Override
    public HipChatAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
        HipChatAction action = HipChatAction.parse(watchId, actionId, parser);
        HipChatAccount account = hipchatService.getAccount(action.account);
        if (account == null) {
            throw new ElasticsearchParseException("could not parse [hipchat] action [{}]. unknown hipchat account [{}]", watchId,
                    action.account);
        }
        account.validateParsedTemplate(watchId, actionId, action.message);
        return action;
    }

    @Override
    public ExecutableHipChatAction createExecutable(HipChatAction action) {
        return new ExecutableHipChatAction(action, actionLogger,  hipchatService, templateEngine);
    }

}
