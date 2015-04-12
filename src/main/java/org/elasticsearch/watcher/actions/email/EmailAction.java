/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.actions.Action;
import org.elasticsearch.watcher.actions.email.service.*;
import org.elasticsearch.watcher.execution.WatchExecutionContext;
import org.elasticsearch.watcher.support.Variables;
import org.elasticsearch.watcher.support.template.TemplateEngine;
import org.elasticsearch.watcher.watch.Payload;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 */
public class EmailAction extends Action<EmailAction.Result> {

    public static final String TYPE = "email";

    final EmailTemplate emailTemplate;
    final Authentication auth;
    final Profile profile;
    final String account;
    final boolean attachPayload;

    final EmailService emailService;
    final TemplateEngine templateEngine;

    public EmailAction(ESLogger logger, EmailTemplate emailTemplate, Authentication auth, Profile profile, String account, boolean attachPayload,
                       EmailService emailService, TemplateEngine templateEngine) {
        super(logger);
        this.emailTemplate = emailTemplate;
        this.auth = auth;
        this.profile = profile;
        this.account = account;
        this.attachPayload = attachPayload;
        this.emailService = emailService;
        this.templateEngine = templateEngine;
    }

    @Override
    public String type() {
        return TYPE;
    }

    protected Result execute(String actionId, WatchExecutionContext ctx, Payload payload) throws IOException {
        try {
            return doExecute(actionId, ctx, payload);
        } catch (Exception e) {
            logger.error("could not send email [{}] for watch [{}]", e, actionId, ctx.watch().name());
            return new Result.Failure("could not execute email action. error: " + e.getMessage());
        }
    }

    protected Result doExecute(String actionId, WatchExecutionContext ctx, Payload payload) throws Exception {
        Map<String, Object> model = Variables.createCtxModel(ctx, payload);

        Email.Builder email = emailTemplate.render(templateEngine, model);
        email.id(ctx.id());

        if (attachPayload) {
            Attachment.Bytes attachment = new Attachment.XContent.Yaml("payload", "payload.yml", payload);
            email.attach(attachment);
        }

        if (ctx.simulateAction(actionId)) {
            return new Result.Simulated(email.build());
        }

        EmailService.EmailSent sent = emailService.send(email.build(), auth, profile, account);
        return new Result.Success(sent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(emailTemplate, auth, profile, account, attachPayload);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final EmailAction other = (EmailAction) obj;
        return Objects.equals(this.emailTemplate, other.emailTemplate)
                && Objects.equals(this.auth, other.auth)
                && Objects.equals(this.profile, other.profile)
                && Objects.equals(this.account, other.account)
                && Objects.equals(this.attachPayload, other.attachPayload);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        emailTemplate.xContentBody(builder, params);
        if (account != null) {
            builder.field(Parser.ACCOUNT_FIELD.getPreferredName(), account);
        }
        if (profile != null) {
            builder.field(Parser.PROFILE_FIELD.getPreferredName(), profile);
        }
        if (auth != null) {
            builder.field(Parser.USER_FIELD.getPreferredName(), auth.user());
            builder.field(Parser.PASSWORD_FIELD.getPreferredName(), new String(auth.password()));
        }
        builder.field(Parser.ATTACH_PAYLOAD_FIELD.getPreferredName(), attachPayload);
        return builder.endObject();
    }

    public static class Parser extends AbstractComponent implements Action.Parser<Result, EmailAction> {

        public static final ParseField ACCOUNT_FIELD = new ParseField("account");
        public static final ParseField PROFILE_FIELD = new ParseField("profile");
        public static final ParseField USER_FIELD = new ParseField("user");
        public static final ParseField PASSWORD_FIELD = new ParseField("password");
        public static final ParseField ATTACH_PAYLOAD_FIELD = new ParseField("attach_payload");
        public static final ParseField EMAIL_FIELD = new ParseField("email");
        public static final ParseField REASON_FIELD = new ParseField("reason");
        public static final ParseField SIMULATED_EMAIL_FIELD = new ParseField("simulated_email");

        private final EmailService emailService;
        private final TemplateEngine templateEngine;

        @Inject
        public Parser(Settings settings, EmailService emailService, TemplateEngine templateEngine) {
            super(settings);
            this.emailService = emailService;
            this.templateEngine = templateEngine;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public EmailAction parse(XContentParser parser) throws IOException {
            EmailTemplate.Parser emailParser = EmailTemplate.parser();
            String user = null;
            String password = null;
            String account = null;
            Profile profile = null;
            boolean attachPayload = false;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (!emailParser.handle(currentFieldName, parser)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        if (ACCOUNT_FIELD.match(currentFieldName)) {
                            account = parser.text();
                        } else if (USER_FIELD.match(currentFieldName)) {
                            user = parser.text();
                        } else if (PASSWORD_FIELD.match(currentFieldName)) {
                            password = parser.text();
                        } else if (PROFILE_FIELD.match(currentFieldName)) {
                            profile = Profile.resolve(parser.text());
                        } else {
                            throw new EmailActionException("could not parse [email] action. unrecognized string field [" + currentFieldName + "]");
                        }
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (ATTACH_PAYLOAD_FIELD.match(currentFieldName)) {
                            attachPayload = parser.booleanValue();
                        } else {
                            throw new EmailActionException("could not parse [email] action. unrecognized boolean field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new EmailActionException("could not parse [email] action. unexpected token [" + token + "]");
                    }
                }
            }

            Authentication auth = user != null ? new Authentication(user, password.toCharArray()) : null;

            return new EmailAction(logger, emailParser.parsedTemplate(), auth, profile, account, attachPayload, emailService, templateEngine);
        }

        @Override
        public EmailAction.Result parseResult(XContentParser parser) throws IOException {
            Boolean success = null;
            Email email = null;
            Email simulatedEmail = null;
            String account = null;
            String reason = null;

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (ACCOUNT_FIELD.match(currentFieldName)) {
                        account = parser.text();
                    } else if (REASON_FIELD.match(currentFieldName)) {
                        reason = parser.text();
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (Action.Result.SUCCESS_FIELD.match(currentFieldName)) {
                            success = parser.booleanValue();
                        } else {
                            throw new EmailException("could not parse email result. unexpected field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new EmailException("could not parse email result. unexpected field [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (EMAIL_FIELD.match(currentFieldName)) {
                        email = Email.parse(parser);
                    } else if (SIMULATED_EMAIL_FIELD.match(currentFieldName)) {
                        simulatedEmail = Email.parse(parser);
                    } else {
                        throw new EmailException("could not parse email result. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new EmailException("could not parse email result. unexpected token [" + token + "]");
                }
            }

            if (simulatedEmail != null) {
                return new Result.Simulated(simulatedEmail);
            }

            if (success == null) {
                throw new EmailException("could not parse email result. expected field [success]");
            }

            return success ? new Result.Success(new EmailService.EmailSent(account, email)) : new Result.Failure(reason);
        }
    }

    public static abstract class Result extends Action.Result {

        public Result(String type, boolean success) {
            super(type, success);
        }

        public static class Success extends Result {

            private final EmailService.EmailSent sent;

            private Success(EmailService.EmailSent sent) {
                super(TYPE, true);
                this.sent = sent;
            }

            @Override
            public XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.ACCOUNT_FIELD.getPreferredName(), sent.account())
                        .field(Parser.EMAIL_FIELD.getPreferredName(), sent.email());
            }

            public String account() {
                return sent.account();
            }

            public Email email() {
                return sent.email();
            }
        }

        public static class Failure extends Result {

            private final String reason;

            public Failure(String reason) {
                super(TYPE, false);
                this.reason = reason;
            }

            public String reason() {
                return reason;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.REASON_FIELD.getPreferredName(), reason);
            }
        }

        public static class Simulated extends Result {

            private final Email email;

            public Email email() {
                return email;
            }

            public Simulated(Email email) {
                super(TYPE, true);
                this.email = email;
            }

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field(Parser.SIMULATED_EMAIL_FIELD.getPreferredName(), email);
            }
        }
    }

    public static class SourceBuilder extends Action.SourceBuilder<SourceBuilder> {

        private final EmailTemplate email;
        private Authentication auth = null;
        private Profile profile = null;
        private String account = null;
        private Boolean attachPayload;

        public SourceBuilder(EmailTemplate email) {
            this.email = email;
        }

        @Override
        public String type() {
            return TYPE;
        }

        public SourceBuilder auth(String username, char[] password) {
            this.auth = new Authentication(username, password);
            return this;
        }

        public SourceBuilder profile(Profile profile) {
            this.profile = profile;
            return this;
        }

        public SourceBuilder account(String account) {
            this.account = account;
            return this;
        }

        public SourceBuilder attachPayload(boolean attachPayload) {
            this.attachPayload = attachPayload;
            return this;
        }

        @Override
        public XContentBuilder actionXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            email.xContentBody(builder, params);
            if (auth != null) {
                builder.field(Parser.USER_FIELD.getPreferredName(), auth.user());
                builder.field(Parser.PASSWORD_FIELD.getPreferredName(), auth.password());
            }
            if (profile != null) {
                builder.field(Parser.PROFILE_FIELD.getPreferredName(), profile);
            }
            if (account != null) {
                builder.field(Parser.ACCOUNT_FIELD.getPreferredName(), account);
            }
            if (attachPayload != null) {
                builder.field(Parser.ATTACH_PAYLOAD_FIELD.getPreferredName(), attachPayload);
            }
            return builder.endObject();
        }
    }

}
