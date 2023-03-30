/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.actions.email;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.core.watcher.actions.Action;
import org.elasticsearch.xpack.core.watcher.actions.ExecutableAction;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.elasticsearch.xpack.watcher.notification.email.Attachment;
import org.elasticsearch.xpack.watcher.notification.email.DataAttachment;
import org.elasticsearch.xpack.watcher.notification.email.Email;
import org.elasticsearch.xpack.watcher.notification.email.EmailService;
import org.elasticsearch.xpack.watcher.notification.email.HtmlSanitizer;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentParser;
import org.elasticsearch.xpack.watcher.notification.email.attachment.EmailAttachmentParser.EmailAttachment;
import org.elasticsearch.xpack.watcher.support.Variables;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.core.Strings.format;

public class ExecutableEmailAction extends ExecutableAction<EmailAction> {

    private static final AtomicLong counter = new AtomicLong(0);

    private final EmailService emailService;
    private final TextTemplateEngine templateEngine;
    private final HtmlSanitizer htmlSanitizer;
    private final Map<String, EmailAttachmentParser<? extends EmailAttachment>> emailAttachmentParsers;

    public ExecutableEmailAction(
        EmailAction action,
        Logger logger,
        EmailService emailService,
        TextTemplateEngine templateEngine,
        HtmlSanitizer htmlSanitizer,
        Map<String, EmailAttachmentParser<? extends EmailAttachment>> emailAttachmentParsers
    ) {
        super(action, logger);
        this.emailService = emailService;
        this.templateEngine = templateEngine;
        this.htmlSanitizer = htmlSanitizer;
        this.emailAttachmentParsers = emailAttachmentParsers;
    }

    public Action.Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        Map<String, Object> model = Variables.createCtxParamsMap(ctx, payload);

        Map<String, Attachment> attachments = new HashMap<>();
        DataAttachment dataAttachment = action.getDataAttachment();
        if (dataAttachment != null) {
            Attachment attachment = dataAttachment.create("data", model);
            attachments.put(attachment.id(), attachment);
        }

        if (action.getAttachments() != null && action.getAttachments().getAttachments().size() > 0) {
            for (EmailAttachment emailAttachment : action.getAttachments().getAttachments()) {
                @SuppressWarnings("unchecked")
                EmailAttachmentParser<EmailAttachment> parser = (EmailAttachmentParser<EmailAttachment>) emailAttachmentParsers.get(
                    emailAttachment.type()
                );
                try {
                    Attachment attachment = parser.toAttachment(ctx, payload, emailAttachment);
                    attachments.put(attachment.id(), attachment);
                } catch (ElasticsearchException | IOException e) {
                    logger().error(() -> format("failed to execute action [%s/%s]", ctx.watch().id(), actionId), e);
                    return new EmailAction.Result.FailureWithException(action.type(), e);
                }
            }
        }

        Email.Builder email = action.getEmail().render(templateEngine, model, htmlSanitizer, attachments);
        // the counter ensures that a different message id is generated, even if the method is called with the same parameters
        // this may happen if a foreach loop is used for this action
        // same message ids will result in emails not being accepted by mail servers and thus have to be prevented at all times
        email.id(actionId + "_" + ctx.id().value() + "_" + Long.toUnsignedString(counter.incrementAndGet()));

        if (ctx.simulateAction(actionId)) {
            return new EmailAction.Result.Simulated(email.build());
        }

        EmailService.EmailSent sent = emailService.send(email.build(), action.getAuth(), action.getProfile(), action.getAccount());
        return new EmailAction.Result.Success(sent.account(), sent.email());
    }
}
