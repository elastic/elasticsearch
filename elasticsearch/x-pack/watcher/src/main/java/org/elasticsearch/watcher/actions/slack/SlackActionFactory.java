/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.slack;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.ActionFactory;
import org.elasticsearch.watcher.actions.hipchat.ExecutableHipChatAction;
import org.elasticsearch.xpack.notification.slack.SlackAccount;
import org.elasticsearch.xpack.notification.slack.SlackService;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;

import java.io.IOException;

/**
 *
 */
public class SlackActionFactory extends ActionFactory<SlackAction, ExecutableSlackAction> {
    private final TextTemplateEngine templateEngine;
    private final SlackService slackService;

    @Inject
    public SlackActionFactory(Settings settings, TextTemplateEngine templateEngine, SlackService slackService) {
        super(Loggers.getLogger(ExecutableHipChatAction.class, settings));
        this.templateEngine = templateEngine;
        this.slackService = slackService;
    }

    @Override
    public String type() {
        return SlackAction.TYPE;
    }

    @Override
    public SlackAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
        SlackAction action = SlackAction.parse(watchId, actionId, parser);
        SlackAccount account = slackService.getAccount(action.account);
        if (account == null) {
            throw new ElasticsearchParseException("could not parse [slack] action [{}]. unknown slack account [{}]", watchId,
                    action.account);
        }
        return action;
    }

    @Override
    public ExecutableSlackAction createExecutable(SlackAction action) {
        return new ExecutableSlackAction(action, actionLogger, slackService, templateEngine);
    }
}
