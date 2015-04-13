/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.logging;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.ActionFactory;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.template.TemplateEngine;

import java.io.IOException;

/**
 *
 */
public class LoggingActionFactory extends ActionFactory<LoggingAction, LoggingAction.Result, ExecutableLoggingAction> {

    private final Settings settings;
    private final TemplateEngine templateEngine;

    @Inject
    public LoggingActionFactory(Settings settings, TemplateEngine templateEngine) {
        super(Loggers.getLogger(ExecutableLoggingAction.class, settings));
        this.settings = settings;
        this.templateEngine = templateEngine;
    }

    @Override
    public String type() {
        return LoggingAction.TYPE;
    }

    @Override
    public LoggingAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
        return LoggingAction.parse(watchId, actionId, parser);
    }

    @Override
    public LoggingAction.Result parseResult(Wid wid, String actionId, XContentParser parser) throws IOException {
        return LoggingAction.Result.parse(wid.watchId(), actionId, parser);
    }

    @Override
    public ExecutableLoggingAction createExecutable(LoggingAction action) {
        return new ExecutableLoggingAction(action, actionLogger, settings, templateEngine);
    }
}
