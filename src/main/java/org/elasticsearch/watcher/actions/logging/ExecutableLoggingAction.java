/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.actions.ExecutableAction;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class ExecutableLoggingAction extends ExecutableAction<LoggingAction, LoggingAction.Result> {

    private final ESLogger textLogger;
    private final TemplateEngine templateEngine;

    ExecutableLoggingAction(LoggingAction action, ESLogger logger, Settings settings, TemplateEngine templateEngine) {
        super(action, logger);
        this.textLogger = action.category != null ? Loggers.getLogger(action.category, settings) : logger;
        this.templateEngine = templateEngine;
    }

    // for tests
    ExecutableLoggingAction(LoggingAction action, ESLogger logger, ESLogger textLogger, TemplateEngine templateEngine) {
        super(action, logger);
        this.textLogger = textLogger;
        this.templateEngine = templateEngine;
    }

    ESLogger textLogger() {
        return textLogger;
    }

    @Override
    protected LoggingAction.Result doExecute(String actionId, WatchExecutionContext ctx, Payload payload) throws IOException {
        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        String loggedText = templateEngine.render(action.text, model);
        if (ctx.simulateAction(actionId)) {
            return new LoggingAction.Result.Simulated(loggedText);
        }

        action.level.log(textLogger, loggedText);
        return new LoggingAction.Result.Success(loggedText);
    }

    @Override
    protected LoggingAction.Result failure(String reason) {
        return new LoggingAction.Result.Failure(reason);
    }
}
