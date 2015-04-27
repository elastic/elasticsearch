/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ActionFactory;
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.execution.Wid;
import org.elasticsearch.watcher.support.template.TemplateEngine;

import java.io.IOException;

/**
 *
 */
public class EmailActionFactory extends ActionFactory<EmailAction, ExecutableEmailAction> {

    private final EmailService emailService;
    private final TemplateEngine templateEngine;
    private final boolean sanitizeHtmlBodyOfEmails;

    private static String SANITIZE_HTML_SETTING = "watcher.actions.email.sanitize_html";

    @Inject
    public EmailActionFactory(Settings settings, EmailService emailService, TemplateEngine templateEngine) {
        super(Loggers.getLogger(ExecutableEmailAction.class, settings));
        this.emailService = emailService;
        this.templateEngine = templateEngine;
        sanitizeHtmlBodyOfEmails = settings.getAsBoolean(SANITIZE_HTML_SETTING, true);
    }

    @Override
    public String type() {
        return EmailAction.TYPE;
    }

    @Override
    public EmailAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
        return EmailAction.parse(watchId, actionId, parser, sanitizeHtmlBodyOfEmails);
    }

    @Override
    public Action.Result parseResult(Wid wid, String actionId, XContentParser parser) throws IOException {
        return EmailAction.Result.parse(wid.watchId(), actionId, parser);
    }

    @Override
    public ExecutableEmailAction createExecutable(EmailAction action) {
        return new ExecutableEmailAction(action, actionLogger, emailService, templateEngine);
    }
}
