/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.actions.logging;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.actions.ActionFactory;

import java.io.IOException;

public class LoggingActionFactory extends ActionFactory {

    private final Settings settings;
    private final TextTemplateEngine templateEngine;

    public LoggingActionFactory(Settings settings, TextTemplateEngine templateEngine) {
        super(Loggers.getLogger(ExecutableLoggingAction.class, settings));
        this.settings = settings;
        this.templateEngine = templateEngine;
    }

    @Override
    public ExecutableLoggingAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        LoggingAction action = LoggingAction.parse(watchId, actionId, parser);
        return new ExecutableLoggingAction(action, actionLogger, settings, templateEngine);
    }
}
