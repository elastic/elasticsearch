/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.logging;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import java.io.IOException;

public class LoggingActionFactory extends ActionFactory {

    private final TextTemplateEngine templateEngine;

    public LoggingActionFactory(TextTemplateEngine templateEngine) {
        super(LogManager.getLogger(ExecutableLoggingAction.class));
        this.templateEngine = templateEngine;
    }

    @Override
    public ExecutableLoggingAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        LoggingAction action = LoggingAction.parse(watchId, actionId, parser);
        return new ExecutableLoggingAction(action, actionLogger, templateEngine);
    }
}
