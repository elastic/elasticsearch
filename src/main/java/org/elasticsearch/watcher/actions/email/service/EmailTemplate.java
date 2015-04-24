/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
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

    public Email.Builder render(TemplateEngine engine, Map<String, Object> model) throws AddressException {
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
        if (htmlBody != null) {
            builder.htmlBody(engine.render(htmlBody, model));
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
        if (textBody != null) {
            builder.field(Email.Field.TEXT_BODY.getPreferredName(), textBody, params);
        }
        if (htmlBody != null) {
            builder.field(Email.Field.HTML_BODY.getPreferredName(), htmlBody, params);
        }
        return builder;
    }

    public static EmailTemplate parse(XContentParser parser) throws IOException{
        EmailTemplate.Parser templateParser = parser();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ((token.isValue() || token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) && currentFieldName != null) {
                if (!templateParser.handle(currentFieldName, parser)) {
                    throw new EmailException("could not parse email template. unrecognized field [" + currentFieldName + "]");
                }
            }
        }
        return templateParser.parsedTemplate();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Parser parser() {
        return new Parser();
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
            return from(new Template(from));
        }

        public Builder from(Template from) {
            this.from = from;
            return this;
        }

        public Builder replyTo(String... replyTo) {
            return replyTo(new Template(Strings.arrayToCommaDelimitedString(replyTo)));
        }

        public Builder replyTo(Template... replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        public Builder priority(Email.Priority priority) {
            return priority(new Template(priority.name()));
        }

        public Builder priority(Template priority) {
            this.priority = priority;
            return this;
        }

        public Builder to(String... to) {
            Template[] templates = new Template[to.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = new Template(to[i]);
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
                templates[i] = new Template(cc[i]);
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
                templates[i] = new Template(bcc[i]);
            }
            return bcc(templates);
        }

        public Builder bcc(Template... bcc) {
            this.bcc = bcc;
            return this;
        }

        public Builder subject(String subject) {
            return subject(new Template(subject));
        }

        public Builder subject(Template subject) {
            this.subject = subject;
            return this;
        }

        public Builder textBody(String text) {
            return textBody(new Template(text));
        }

        public Builder textBody(Template text) {
            this.textBody = text;
            return this;
        }

        public Builder htmlBody(String html) {
            return htmlBody(new Template(html));
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
            } else if (Email.Field.TEXT_BODY.match(fieldName)) {
                builder.textBody(Template.parse(parser));
            } else if (Email.Field.HTML_BODY.match(fieldName)) {
                builder.htmlBody(Template.parse(parser));
            } else {
                return false;
            }
            return true;
        }

        public EmailTemplate parsedTemplate() {
            return builder.build();
        }
    }
}
