/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.slack;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.slack.SentMessages;
import org.elasticsearch.xpack.watcher.notification.slack.SlackAccount;
import org.elasticsearch.xpack.watcher.notification.slack.SlackService;
import org.elasticsearch.xpack.watcher.notification.slack.message.SlackMessage;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.util.Map;

public class ExecutableSlackAction extends ExecutableAction<SlackAction> {

    private final TextTemplateEngine templateEngine;
    private final SlackService slackService;

    public ExecutableSlackAction(SlackAction action, Logger logger, SlackService slackService, TextTemplateEngine templateEngine) {
        super(action, logger);
        this.slackService = slackService;
        this.templateEngine = templateEngine;
    }

    @Override
    public Action.Result execute(final String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {

        SlackAccount account = slackService.getAccount(action.account);

        if (account == null) {
            // the account associated with this action was deleted
            throw new IllegalStateException("account [" + action.account + "] was not found. perhaps it was deleted");
        }

        Map<String, Object> model = Variables.createCtxParamsMap(ctx, payload);
        SlackMessage message = action.message.render(ctx.id().watchId(), actionId, templateEngine, model, account.getMessageDefaults());

        if (ctx.simulateAction(actionId)) {
            return new SlackAction.Result.Simulated(message);
        }

        SentMessages sentMessages = account.send(message, action.proxy);
        return new SlackAction.Result.Executed(sentMessages);
    }

}
