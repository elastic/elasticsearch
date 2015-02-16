/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email;

import org.elasticsearch.alerts.ExecutionContext;
import org.elasticsearch.alerts.Payload;
import org.elasticsearch.alerts.actions.Action;
import org.elasticsearch.alerts.actions.email.service.*;
import org.elasticsearch.alerts.support.StringTemplateUtils;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class EmailAction extends Action<EmailAction.Result> {

    public static final String TYPE = "email";

    private final Email.Builder email;
    private final Authentication auth;
    private final Profile profile;
    private final String account;
    private final StringTemplateUtils.Template subject;
    private final StringTemplateUtils.Template textBody;
    private final StringTemplateUtils.Template htmlBody;
    private final boolean attachPayload;

    private final EmailService emailService;
    private final StringTemplateUtils templateUtils;

    public EmailAction(ESLogger logger, EmailService emailService, StringTemplateUtils templateUtils,
                          Email.Builder email, Authentication auth, Profile profile, String account,
                          StringTemplateUtils.Template subject, StringTemplateUtils.Template textBody,
                          StringTemplateUtils.Template htmlBody, boolean attachPayload) {
        super(logger);
        this.emailService = emailService;
        this.templateUtils = templateUtils;
        this.email = email;
        this.auth = auth;
        this.profile = profile;
        this.account = account;
        this.subject = subject;
        this.textBody = textBody;
        this.htmlBody = htmlBody;
        this.attachPayload = attachPayload;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Result execute(ExecutionContext ctx, Payload payload) throws IOException {
        email.id(ctx.id());

        Map<String, Object> alertParams = new HashMap<>();
        alertParams.put(Action.ALERT_NAME_VARIABLE_NAME, ctx.alert().name());
        alertParams.put(RESPONSE_VARIABLE_NAME, payload.data());

        String text = templateUtils.executeTemplate(subject, alertParams);
        email.subject(text);

        text = templateUtils.executeTemplate(textBody, alertParams);
        email.textBody(text);

        if (htmlBody != null) {
            text = templateUtils.executeTemplate(htmlBody, alertParams);
            email.htmlBody(text);
        }

        if (attachPayload) {
            Attachment.Bytes attachment = new Attachment.XContent.Yaml("payload", "payload.yml", "alert execution output", payload);
            email.attach(attachment);
        }

        try {
            EmailService.EmailSent sent = emailService.send(email.build(), auth, profile, account);
            return new Result.Success(sent);
        } catch (EmailException ee) {
            logger.error("could not send email for alert [{}]", ee, ctx.alert().name());
            return new Result.Failure("could not send email for alert [" + ctx.alert().name() + "]. error: " + ee.getMessage());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (account != null) {
            builder.field(Parser.ACCOUNT_FIELD.getPreferredName(), account);
        }
        if (profile != null) {
            builder.field(Parser.PROFILE_FIELD.getPreferredName(), profile);
        }
        builder.field(Email.TO_FIELD.getPreferredName(), (ToXContent) email.to());
        if (subject != null) {
            StringTemplateUtils.writeTemplate(Email.SUBJECT_FIELD.getPreferredName(), subject, builder, params);
        }
        if (textBody != null) {
            StringTemplateUtils.writeTemplate(Email.TEXT_BODY_FIELD.getPreferredName(), textBody, builder, params);
        }
        return builder.endObject();
    }

    public static class Parser extends AbstractComponent implements Action.Parser<EmailAction> {

        public static final ParseField ACCOUNT_FIELD = new ParseField("account");
        public static final ParseField PROFILE_FIELD = new ParseField("profile");
        public static final ParseField USER_FIELD = new ParseField("user");
        public static final ParseField PASSWORD_FIELD = new ParseField("password");
        public static final ParseField ATTACH_PAYLOAD_FIELD = new ParseField("attach_payload");
        public static final ParseField EMAIL_FIELD = new ParseField("email");
        public static final ParseField REASON_FIELD = new ParseField("reason");

        private final StringTemplateUtils templateUtils;
        private final EmailService emailService;

        @Inject
        public Parser(Settings settings, EmailService emailService, StringTemplateUtils templateUtils) {
            super(settings);
            this.emailService = emailService;
            this.templateUtils = templateUtils;
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public EmailAction parse(XContentParser parser) throws IOException {
            String user = null;
            String password = null;
            String account = null;
            Profile profile = null;
            Email.Builder email = Email.builder();
            StringTemplateUtils.Template subject = null;
            StringTemplateUtils.Template textBody = null;
            StringTemplateUtils.Template htmlBody = null;
            boolean attachPayload = false;

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if ((token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) && currentFieldName != null) {
                    if (Email.FROM_FIELD.match(currentFieldName)) {
                        email.from(Email.Address.parse(currentFieldName, token, parser));
                    } else if (Email.REPLY_TO_FIELD.match(currentFieldName)) {
                        email.replyTo(Email.AddressList.parse(currentFieldName, token, parser));
                    } else if (Email.TO_FIELD.match(currentFieldName)) {
                        email.to(Email.AddressList.parse(currentFieldName, token, parser));
                    } else if (Email.CC_FIELD.match(currentFieldName)) {
                        email.cc(Email.AddressList.parse(currentFieldName, token, parser));
                    } else if (Email.BCC_FIELD.match(currentFieldName)) {
                        email.bcc(Email.AddressList.parse(currentFieldName, token, parser));
                    } else if (Email.SUBJECT_FIELD.match(currentFieldName)) {
                        subject = StringTemplateUtils.readTemplate(parser);
                    } else if (Email.TEXT_BODY_FIELD.match(currentFieldName)) {
                        textBody = StringTemplateUtils.readTemplate(parser);
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        if (Email.PRIORITY_FIELD.match(currentFieldName)) {
                            email.priority(Email.Priority.resolve(parser.text()));
                        }  else if (Email.HTML_BODY_FIELD.match(currentFieldName)) {
                            htmlBody = StringTemplateUtils.readTemplate(parser);
                        } else if (ACCOUNT_FIELD.match(currentFieldName)) {
                            account = parser.text();
                        } else if (USER_FIELD.match(currentFieldName)) {
                            user = parser.text();
                        } else if (PASSWORD_FIELD.match(currentFieldName)) {
                            password = parser.text();
                        } else if (PROFILE_FIELD.match(currentFieldName)) {
                            profile = Profile.resolve(parser.text());
                        } else {
                            throw new EmailException("could not parse email action. unrecognized string field [" + currentFieldName + "]");
                        }
                    } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                        if (ATTACH_PAYLOAD_FIELD.match(currentFieldName)) {
                            attachPayload = parser.booleanValue();
                        } else {
                            throw new EmailException("could not parse email action. unrecognized boolean field [" + currentFieldName + "]");
                        }
                    } else {
                        throw new EmailException("could not parse email action. unexpected token [" + token + "]");
                    }
                }
            }

            if (email.to() == null || email.to().isEmpty()) {
                throw new EmailException("could not parse email action. [to] was not found or was empty");
            }

            Authentication auth = new Authentication(user, password);
            return new EmailAction(logger, emailService, templateUtils, email, auth, profile, account, subject, textBody, htmlBody, attachPayload);
        }

        @Override
        public EmailAction.Result parseResult(XContentParser parser) throws IOException {

            String currentFieldName = null;
            XContentParser.Token token;
            Boolean success = null;
            Email email = null;
            String account = null;
            String reason = null;


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
                    } else {
                        throw new EmailException("could not parse email result. unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new EmailException("could not parse email result. unexpected token [" + token + "]");
                }
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
                return builder.field("account", sent.account())
                        .field("email", sent.email());
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

            @Override
            protected XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
                return builder.field("reason", reason);
            }
        }
    }



}
