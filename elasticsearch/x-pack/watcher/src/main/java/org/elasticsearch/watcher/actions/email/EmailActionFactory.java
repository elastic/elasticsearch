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
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.actions.email.service.HtmlSanitizer;
import org.elasticsearch.watcher.actions.email.service.attachment.EmailAttachmentParser;
import org.elasticsearch.watcher.actions.email.service.attachment.EmailAttachmentsParser;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class EmailActionFactory extends ActionFactory<EmailAction, ExecutableEmailAction> {

    private final EmailService emailService;
    private final TextTemplateEngine templateEngine;
    private final HtmlSanitizer htmlSanitizer;
    private final EmailAttachmentsParser emailAttachmentsParser;
    private final Map<String, EmailAttachmentParser> emailAttachmentParsers;

    @Inject
    public EmailActionFactory(Settings settings, EmailService emailService, TextTemplateEngine templateEngine, HtmlSanitizer htmlSanitizer,
                              EmailAttachmentsParser emailAttachmentsParser, Map<String, EmailAttachmentParser> emailAttachmentParsers) {
        super(Loggers.getLogger(ExecutableEmailAction.class, settings));
        this.emailService = emailService;
        this.templateEngine = templateEngine;
        this.htmlSanitizer = htmlSanitizer;
        this.emailAttachmentsParser = emailAttachmentsParser;
        this.emailAttachmentParsers = emailAttachmentParsers;
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
        return new ExecutableEmailAction(action, actionLogger, emailService, templateEngine, htmlSanitizer, emailAttachmentParsers);
    }
}
