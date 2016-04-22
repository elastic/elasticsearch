/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.ExecutableAction;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.text.TextTemplateEngine;
import org.elasticsearch.watcher.watch.Payload;
import org.elasticsearch.xpack.notification.email.Attachment;
import org.elasticsearch.xpack.notification.email.DataAttachment;
import org.elasticsearch.xpack.notification.email.Email;
import org.elasticsearch.xpack.notification.email.EmailService;
import org.elasticsearch.xpack.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.notification.email.attachment.EmailAttachmentParser;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class ExecutableEmailAction extends ExecutableAction<EmailAction> {

    final EmailService emailService;
    final TextTemplateEngine templateEngine;
    final HtmlSanitizer htmlSanitizer;
    private final Map<String, EmailAttachmentParser> emailAttachmentParsers;

    public ExecutableEmailAction(EmailAction action, ESLogger logger, EmailService emailService, TextTemplateEngine templateEngine,
                                 HtmlSanitizer htmlSanitizer, Map<String, EmailAttachmentParser> emailAttachmentParsers) {
        super(action, logger);
        this.emailService = emailService;
        this.templateEngine = templateEngine;
        this.htmlSanitizer = htmlSanitizer;
        this.emailAttachmentParsers = emailAttachmentParsers;
    }

    public Action.Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        Map<String, Attachment> attachments = new HashMap<>();
        DataAttachment dataAttachment = action.getDataAttachment();
        if (dataAttachment != null) {
            Attachment attachment = dataAttachment.create("data", model);
            attachments.put(attachment.id(), attachment);
        }

        if (action.getAttachments() != null && action.getAttachments().getAttachments().size() > 0) {
            for (EmailAttachmentParser.EmailAttachment emailAttachment : action.getAttachments().getAttachments()) {
                EmailAttachmentParser parser = emailAttachmentParsers.get(emailAttachment.type());
                try {
                    Attachment attachment = parser.toAttachment(ctx, payload, emailAttachment);
                    attachments.put(attachment.id(), attachment);
                } catch (ElasticsearchException e) {
                    return new EmailAction.Result.Failure(action.type(), e.getMessage());
                }
            }
        }

        Email.Builder email = action.getEmail().render(templateEngine, model, htmlSanitizer, attachments);
        email.id(ctx.id().value());

        if (ctx.simulateAction(actionId)) {
            return new EmailAction.Result.Simulated(email.build());
        }

        EmailService.EmailSent sent = emailService.send(email.build(), action.getAuth(), action.getProfile(), action.getAccount());
        return new EmailAction.Result.Success(sent.account(), sent.email());
    }
}
