/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.jira;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.actions.ActionFactory;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.jira.JiraService;

import java.io.IOException;

public class JiraActionFactory extends ActionFactory {

    private final TextTemplateEngine templateEngine;
    private final JiraService jiraService;

    public JiraActionFactory(TextTemplateEngine templateEngine, JiraService jiraService) {
        super(LogManager.getLogger(ExecutableJiraAction.class));
        this.templateEngine = templateEngine;
        this.jiraService = jiraService;
    }

    @Override
    public ExecutableJiraAction parseExecutable(String watchId, String actionId, XContentParser parser) throws IOException {
        JiraAction action = JiraAction.parse(watchId, actionId, parser);
        jiraService.getAccount(action.getAccount()); // for validation -- throws exception if account not present
        return new ExecutableJiraAction(action, actionLogger, jiraService, templateEngine);
    }
}
