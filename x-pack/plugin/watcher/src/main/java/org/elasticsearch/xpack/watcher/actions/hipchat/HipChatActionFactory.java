/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.hipchat;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatAccount;
import org.elasticsearch.xpack.watcher.notification.hipchat.HipChatService;

import java.io.IOException;

public class HipChatActionFactory extends ActionFactory {

    private final TextTemplateEngine templateEngine;
    private final HipChatService hipchatService;

    public HipChatActionFactory(TextTemplateEngine templateEngine, HipChatService hipchatService) {
        super(LogManager.getLogger(ExecutableHipChatAction.class));
        this.templateEngine = templateEngine;
        this.hipchatService = hipchatService;
    }

    @Override
    public ExecutableHipChatAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        HipChatAction action = HipChatAction.parse(watchId, actionId, parser);
        HipChatAccount account = hipchatService.getAccount(action.account);
        account.validateParsedTemplate(watchId, actionId, action.message);
        return new ExecutableHipChatAction(action, actionLogger,  hipchatService, templateEngine);
    }
}
