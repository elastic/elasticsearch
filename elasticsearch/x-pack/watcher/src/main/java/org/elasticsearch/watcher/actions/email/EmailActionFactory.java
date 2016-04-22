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
import org.elasticsearch.watcher.actions.ActionFactory;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.notification.email.attachment.EmailAttachmentsParser;

import java.io.IOException;

/**
 *
 */
public class EmailActionFactory extends ActionFactory<EmailAction, ExecutableEmailAction> {

    private final EmailService emailService;
    private final TextTemplateEngine templateEngine;
    private final HtmlSanitizer htmlSanitizer;
    private final EmailAttachmentsParser emailAttachmentsParser;

    @Inject
    public EmailActionFactory(Settings settings, EmailService emailService, TextTemplateEngine templateEngine, HtmlSanitizer htmlSanitizer,
                              EmailAttachmentsParser emailAttachmentsParser) {
        super(Loggers.getLogger(ExecutableEmailAction.class, settings));
        this.emailService = emailService;
        this.templateEngine = templateEngine;
        this.htmlSanitizer = htmlSanitizer;
        this.emailAttachmentsParser = emailAttachmentsParser;
    }

    @Override
    public String type() {
        return EmailAction.TYPE;
    }

    @Override
    public EmailAction parseAction(String watchId, String actionId, XContentParser parser) throws IOException {
        return EmailAction.parse(watchId, actionId, parser, emailAttachmentsParser);
    }

    @Override
    public ExecutableEmailAction createExecutable(EmailAction action) {
        return new ExecutableEmailAction(action, actionLogger, emailService, templateEngine, htmlSanitizer,
                emailAttachmentsParser.getParsers());
    }
}
