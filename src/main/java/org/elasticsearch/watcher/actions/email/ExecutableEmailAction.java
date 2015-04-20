/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.watcher.actions.ExecutableAction;
import org.elasticsearch.watcher.actions.email.service.Attachment;
import org.elasticsearch.watcher.actions.email.service.Email;
import org.elasticsearch.watcher.actions.email.service.EmailService;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.util.Map;

/**
 */
public class ExecutableEmailAction extends ExecutableAction<EmailAction, EmailAction.Result> {

    final EmailService emailService;
    final TemplateEngine templateEngine;

    public ExecutableEmailAction(EmailAction action, ESLogger logger, EmailService emailService, TemplateEngine templateEngine) {
        super(action, logger);
        this.emailService = emailService;
        this.templateEngine = templateEngine;
    }

    protected EmailAction.Result doExecute(String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        Email.Builder email = action.getEmail().render(templateEngine, model);
        email.id(ctx.id().value());

        if (action.getAttachData()) {
            Attachment.Bytes attachment = new Attachment.XContent.Yaml("data", "data.yml", new Payload.Simple(model));
            email.attach(attachment);
        }

        if (ctx.simulateAction(actionId)) {
            return new EmailAction.Result.Simulated(email.build());
        }

        EmailService.EmailSent sent = emailService.send(email.build(), action.getAuth(), action.getProfile(), action.getAccount());
        return new EmailAction.Result.Success(sent.account(), sent.email());
    }

    @Override
    protected EmailAction.Result failure(String reason) {
        return new EmailAction.Result.Failure(reason);
    }
}
