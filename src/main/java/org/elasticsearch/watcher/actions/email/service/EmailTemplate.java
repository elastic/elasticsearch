/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.template.Template;
import org.elasticsearch.watcher.support.template.TemplateEngine;

import javax.mail.internet.AddressException;
import java.io.IOException;
import java.util.*;

/**
 *
 */
public class EmailTemplate implements ToXContent {

    final Template from;
    final Template[] replyTo;
    final Template priority;
    final Template[] to;
    final Template[] cc;
    final Template[] bcc;
    final Template subject;
    final Template textBody;
    final Template htmlBody;

    public EmailTemplate(Template from, Template[] replyTo, Template priority, Template[] to,
                         Template[] cc, Template[] bcc, Template subject, Template textBody,
                         Template htmlBody) {
        this.from = from;
        this.replyTo = replyTo;
        this.priority = priority;
        this.to = to;
        this.cc = cc;
        this.bcc = bcc;
        this.subject = subject;
        this.textBody = textBody;
        this.htmlBody = htmlBody;
    }

    public Template from() {
        return from;
    }

    public Template[] replyTo() {
        return replyTo;
    }

    public Template priority() {
        return priority;
    }

    public Template[] to() {
        return to;
    }

    public Template[] cc() {
        return cc;
    }

    public Template[] bcc() {
        return bcc;
    }

    public Template subject() {
        return subject;
    }

    public Template textBody() {
        return textBody;
    }

    public Template htmlBody() {
        return htmlBody;
    }

    public Email.Builder render(TemplateEngine engine, Map<String, Object> model, HtmlSanitizer htmlSanitizer, Map<String, Attachment> attachments) throws AddressException {
        Email.Builder builder = Email.builder();
        if (from != null) {
            builder.from(engine.render(from, model));
        }
        if (replyTo != null) {
            Email.AddressList addresses = templatesToAddressList(engine, replyTo, model);
            builder.replyTo(addresses);
        }
        if (priority != null) {
            builder.priority(Email.Priority.resolve(engine.render(priority, model)));
        }
        if (to != null) {
            Email.AddressList addresses = templatesToAddressList(engine, to, model);
            builder.to(addresses);
        }
        if (cc != null) {
            Email.AddressList addresses = templatesToAddressList(engine, cc, model);
            builder.cc(addresses);
        }
        if (bcc != null) {
            Email.AddressList addresses = templatesToAddressList(engine, bcc, model);
            builder.bcc(addresses);
        }
        if (subject != null) {
            builder.subject(engine.render(subject, model));
        }
        if (textBody != null) {
            builder.textBody(engine.render(textBody, model));
        }
        if (attachments != null) {
            for (Attachment attachment : attachments.values()) {
                builder.attach(attachment);
            }
        }
        if (htmlBody != null) {
            String renderedHtml = engine.render(htmlBody, model);
            renderedHtml = htmlSanitizer.sanitize(renderedHtml);
            builder.htmlBody(renderedHtml);
        }
        return builder;
    }

    private static Email.AddressList templatesToAddressList(TemplateEngine engine, Template[] templates, Map<String, Object> model) throws AddressException {
        List<Email.Address> addresses = new ArrayList<>(templates.length);
        for (Template template : templates) {
            addresses.add(new Email.Address(engine.render(template, model)));
        }
        return new Email.AddressList(addresses);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmailTemplate that = (EmailTemplate) o;
        return Objects.equals(from, that.from) &&
                Arrays.equals(replyTo, that.replyTo) &&
                Objects.equals(priority, that.priority) &&
                Arrays.equals(to, that.to) &&
                Arrays.equals(cc, that.cc) &&
                Arrays.equals(bcc, that.bcc) &&
                Objects.equals(subject, that.subject) &&
                Objects.equals(textBody, that.textBody) &&
                Objects.equals(htmlBody, that.htmlBody);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, replyTo, priority, to, cc, bcc, subject, textBody, htmlBody);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        xContentBody(builder, params);
        return builder.endObject();
    }

    public XContentBuilder xContentBody(XContentBuilder builder, Params params) throws IOException {
        if (from != null) {
            builder.field(Email.Field.FROM.getPreferredName(), from, params);
        }
        if (replyTo != null) {
            builder.startArray(Email.Field.REPLY_TO.getPreferredName());
            for (Template template : replyTo) {
                template.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (priority != null) {
            builder.field(Email.Field.PRIORITY.getPreferredName(), priority, params);
        }
        if (to != null) {
            builder.startArray(Email.Field.TO.getPreferredName());
            for (Template template : to) {
                template.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (cc != null) {
            builder.startArray(Email.Field.CC.getPreferredName());
            for (Template template : cc) {
                template.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (bcc != null) {
            builder.startArray(Email.Field.BCC.getPreferredName());
            for (Template template : bcc) {
                template.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (subject != null) {
            builder.field(Email.Field.SUBJECT.getPreferredName(), subject, params);
        }
        if (textBody != null || htmlBody != null) {
            builder.startObject(Email.Field.BODY.getPreferredName());
            if (textBody != null) {
                builder.field(Email.Field.BODY_TEXT.getPreferredName(), textBody, params);
            }
            if (htmlBody != null) {
                builder.field(Email.Field.BODY_HTML.getPreferredName(), htmlBody, params);
            }
            builder.endObject();
        }
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Template from;
        private Template[] replyTo;
        private Template priority;
        private Template[] to;
        private Template[] cc;
        private Template[] bcc;
        private Template subject;
        private Template textBody;
        private Template htmlBody;

        private Builder() {
        }

        public Builder from(String from) {
            return from(Template.inline(from));
        }

        public Builder from(Template.Builder from) {
            return from(from.build());
        }

        public Builder from(Template from) {
            this.from = from;
            return this;
        }

        public Builder replyTo(String... replyTo) {
            Template[] templates = new Template[replyTo.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = Template.defaultType(replyTo[i]).build();
            }
            return replyTo(templates);
        }

        public Builder replyTo(Template.Builder... replyTo) {
            Template[] templates = new Template[replyTo.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = replyTo[i].build();
            }
            return replyTo(templates);
        }

        public Builder replyTo(Template... replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        public Builder priority(Email.Priority priority) {
            return priority(Template.inline(priority.name()));
        }

        public Builder priority(Template.Builder priority) {
            return priority(priority.build());
        }

        public Builder priority(Template priority) {
            this.priority = priority;
            return this;
        }

        public Builder to(String... to) {
            Template[] templates = new Template[to.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = Template.defaultType(to[i]).build();
            }
            return to(templates);
        }

        public Builder to(Template.Builder... to) {
            Template[] templates = new Template[to.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = to[i].build();
            }
            return to(templates);
        }

        public Builder to(Template... to) {
            this.to = to;
            return this;
        }

        public Builder cc(String... cc) {
            Template[] templates = new Template[cc.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = Template.defaultType(cc[i]).build();
            }
            return cc(templates);
        }

        public Builder cc(Template.Builder... cc) {
            Template[] templates = new Template[cc.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = cc[i].build();
            }
            return cc(templates);
        }

        public Builder cc(Template... cc) {
            this.cc = cc;
            return this;
        }

        public Builder bcc(String... bcc) {
            Template[] templates = new Template[bcc.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = Template.defaultType(bcc[i]).build();
            }
            return bcc(templates);
        }

        public Builder bcc(Template.Builder... bcc) {
            Template[] templates = new Template[bcc.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = bcc[i].build();
            }
            return bcc(templates);
        }

        public Builder bcc(Template... bcc) {
            this.bcc = bcc;
            return this;
        }

        public Builder subject(String subject) {
            return subject(Template.defaultType(subject));
        }

        public Builder subject(Template.Builder subject) {
            return subject(subject.build());
        }

        public Builder subject(Template subject) {
            this.subject = subject;
            return this;
        }

        public Builder textBody(String text) {
            return textBody(Template.defaultType(text));
        }

        public Builder textBody(Template.Builder text) {
            return textBody(text.build());
        }

        public Builder textBody(Template text) {
            this.textBody = text;
            return this;
        }

        public Builder htmlBody(String html) {
            return htmlBody(Template.defaultType(html));
        }

        public Builder htmlBody(Template.Builder html) {
            return htmlBody(html.build());
        }

        public Builder htmlBody(Template html) {
            this.htmlBody = html;
            return this;
        }

        public EmailTemplate build() {
            return new EmailTemplate(from, replyTo, priority, to, cc, bcc, subject, textBody, htmlBody);
        }
    }

    public static class Parser {

        private final EmailTemplate.Builder builder = builder();

        public boolean handle(String fieldName, XContentParser parser) throws IOException {
            if (Email.Field.FROM.match(fieldName)) {
                builder.from(Template.parse(parser));
            } else if (Email.Field.REPLY_TO.match(fieldName)) {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    List<Template> templates = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        templates.add(Template.parse(parser));
                    }
                    builder.replyTo(templates.toArray(new Template[templates.size()]));
                } else {
                    builder.replyTo(Template.parse(parser));
                }
            } else if (Email.Field.TO.match(fieldName)) {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    List<Template> templates = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        templates.add(Template.parse(parser));
                    }
                    builder.to(templates.toArray(new Template[templates.size()]));
                } else {
                    builder.to(Template.parse(parser));
                }
            } else if (Email.Field.CC.match(fieldName)) {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    List<Template> templates = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        templates.add(Template.parse(parser));
                    }
                    builder.cc(templates.toArray(new Template[templates.size()]));
                } else {
                    builder.cc(Template.parse(parser));
                }
            } else if (Email.Field.BCC.match(fieldName)) {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    List<Template> templates = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        templates.add(Template.parse(parser));
                    }
                    builder.bcc(templates.toArray(new Template[templates.size()]));
                } else {
                    builder.bcc(Template.parse(parser));
                }
            } else if (Email.Field.PRIORITY.match(fieldName)) {
                builder.priority(Template.parse(parser));
            } else if (Email.Field.SUBJECT.match(fieldName)) {
                builder.subject(Template.parse(parser));
            } else if (Email.Field.BODY.match(fieldName)) {
                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    builder.textBody(Template.parse(parser));
                } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    XContentParser.Token token;
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (currentFieldName == null) {
                            throw new ParseException("could not parse email template. empty [{}] field", fieldName);
                        } else if (Email.Field.BODY_TEXT.match(currentFieldName)) {
                            builder.textBody(Template.parse(parser));
                        } else if (Email.Field.BODY_HTML.match(currentFieldName)) {
                            builder.htmlBody(Template.parse(parser));
                        } else {
                            throw new ParseException("could not parse email template. unknown field [{}.{}] field", fieldName, currentFieldName);
                        }
                    }
                }
            } else {
                return false;
            }
            return true;
        }

        public EmailTemplate parsedTemplate() {
            return builder.build();
        }
    }

    public static class ParseException extends WatcherException {

        public ParseException(String msg, Object... args) {
            super(msg, args);
        }

        public ParseException(String msg, Throwable cause, Object... args) {
            super(msg, cause, args);
        }
    }


}
