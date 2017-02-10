/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.slack;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.notification.slack.SlackService;
import org.elasticsearch.xpack.watcher.actions.ActionFactory;

import java.io.IOException;

public class SlackActionFactory extends ActionFactory {
    private final TextTemplateEngine templateEngine;
    private final SlackService slackService;

    public SlackActionFactory(Settings settings, TextTemplateEngine templateEngine, SlackService slackService) {
        super(Loggers.getLogger(ExecutableSlackAction.class, settings));
        this.templateEngine = templateEngine;
        this.slackService = slackService;
    }

    @Override
    public ExecutableSlackAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        SlackAction action = SlackAction.parse(watchId, actionId, parser);
        slackService.getAccount(action.account); // for validation -- throws exception if account not present
        return new ExecutableSlackAction(action, actionLogger, slackService, templateEngine);
    }
}
