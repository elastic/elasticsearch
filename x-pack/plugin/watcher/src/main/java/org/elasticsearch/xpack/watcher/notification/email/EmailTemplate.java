/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;

import javax.mail.internet.AddressException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class EmailTemplate implements ToXContentObject {

    final TextTemplate from;
    final TextTemplate[] replyTo;
    final TextTemplate priority;
    final TextTemplate[] to;
    final TextTemplate[] cc;
    final TextTemplate[] bcc;
    final TextTemplate subject;
    final TextTemplate textBody;
    final TextTemplate htmlBody;

    public EmailTemplate(TextTemplate from, TextTemplate[] replyTo, TextTemplate priority, TextTemplate[] to,
                         TextTemplate[] cc, TextTemplate[] bcc, TextTemplate subject, TextTemplate textBody,
                         TextTemplate htmlBody) {
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

    public TextTemplate from() {
        return from;
    }

    public TextTemplate[] replyTo() {
        return replyTo;
    }

    public TextTemplate priority() {
        return priority;
    }

    public TextTemplate[] to() {
        return to;
    }

    public TextTemplate[] cc() {
        return cc;
    }

    public TextTemplate[] bcc() {
        return bcc;
    }

    public TextTemplate subject() {
        return subject;
    }

    public TextTemplate textBody() {
        return textBody;
    }

    public TextTemplate htmlBody() {
        return htmlBody;
    }

    public Email.Builder render(TextTemplateEngine engine, Map<String, Object> model, HtmlSanitizer htmlSanitizer,
                                Map<String, Attachment> attachments) throws AddressException {
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

        Set<String> warnings = new HashSet<>(1);
        if (attachments != null) {
            for (Attachment attachment : attachments.values()) {
                builder.attach(attachment);
                warnings.addAll(attachment.getWarnings());
            }
        }

        String htmlWarnings = "";
        String textWarnings = "";
        if(warnings.isEmpty() == false){
            StringBuilder textWarningBuilder = new StringBuilder();
            StringBuilder htmlWarningBuilder = new StringBuilder();
            warnings.forEach(w ->
            {
                if(Strings.isNullOrEmpty(w) == false) {
                    textWarningBuilder.append(w).append("\n");
                    htmlWarningBuilder.append(w).append("<br>");
                }
            });
            textWarningBuilder.append("\n");
            htmlWarningBuilder.append("<br>");
            htmlWarnings = htmlWarningBuilder.toString();
            textWarnings = textWarningBuilder.toString();
        }
        if (textBody != null) {
            builder.textBody(textWarnings + engine.render(textBody, model));
        }

        if (htmlBody != null) {
            String renderedHtml = htmlWarnings + engine.render(htmlBody, model);
            renderedHtml = htmlSanitizer.sanitize(renderedHtml);
            builder.htmlBody(renderedHtml);
        }

        if(htmlBody == null && textBody == null && Strings.isNullOrEmpty(textWarnings) == false){
            builder.textBody(textWarnings);
        }

        return builder;
    }

    private static Email.AddressList templatesToAddressList(TextTemplateEngine engine, TextTemplate[] templates,
                                                            Map<String, Object> model) throws AddressException {
        List<Email.Address> addresses = new ArrayList<>(templates.length);
        for (TextTemplate template : templates) {
            Email.AddressList.parse(engine.render(template, model)).forEach(addresses::add);
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
            for (TextTemplate template : replyTo) {
                template.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (priority != null) {
            builder.field(Email.Field.PRIORITY.getPreferredName(), priority, params);
        }
        if (to != null) {
            builder.startArray(Email.Field.TO.getPreferredName());
            for (TextTemplate template : to) {
                template.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (cc != null) {
            builder.startArray(Email.Field.CC.getPreferredName());
            for (TextTemplate template : cc) {
                template.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (bcc != null) {
            builder.startArray(Email.Field.BCC.getPreferredName());
            for (TextTemplate template : bcc) {
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

        private TextTemplate from;
        private TextTemplate[] replyTo;
        private TextTemplate priority;
        private TextTemplate[] to;
        private TextTemplate[] cc;
        private TextTemplate[] bcc;
        private TextTemplate subject;
        private TextTemplate textBody;
        private TextTemplate htmlBody;

        private Builder() {
        }

        public Builder from(String from) {
            return from(new TextTemplate(from));
        }

        public Builder from(TextTemplate from) {
            this.from = from;
            return this;
        }

        public Builder replyTo(String... replyTo) {
            TextTemplate[] templates = new TextTemplate[replyTo.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = new TextTemplate(replyTo[i]);
            }
            return replyTo(templates);
        }

        public Builder replyTo(TextTemplate... replyTo) {
            this.replyTo = replyTo;
            return this;
        }

        public Builder priority(Email.Priority priority) {
            return priority(new TextTemplate(priority.name()));
        }

        public Builder priority(TextTemplate priority) {
            this.priority = priority;
            return this;
        }

        public Builder to(String... to) {
            TextTemplate[] templates = new TextTemplate[to.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = new TextTemplate(to[i]);
            }
            return to(templates);
        }

        public Builder to(TextTemplate... to) {
            this.to = to;
            return this;
        }

        public Builder cc(String... cc) {
            TextTemplate[] templates = new TextTemplate[cc.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = new TextTemplate(cc[i]);
            }
            return cc(templates);
        }

        public Builder cc(TextTemplate... cc) {
            this.cc = cc;
            return this;
        }

        public Builder bcc(String... bcc) {
            TextTemplate[] templates = new TextTemplate[bcc.length];
            for (int i = 0; i < templates.length; i++) {
                templates[i] = new TextTemplate(bcc[i]);
            }
            return bcc(templates);
        }

        public Builder bcc(TextTemplate... bcc) {
            this.bcc = bcc;
            return this;
        }

        public Builder subject(String subject) {
            return subject(new TextTemplate(subject));
        }

        public Builder subject(TextTemplate subject) {
            this.subject = subject;
            return this;
        }

        public Builder textBody(String text) {
            return textBody(new TextTemplate(text));
        }

        public Builder textBody(TextTemplate text) {
            this.textBody = text;
            return this;
        }

        public Builder htmlBody(String html) {
            return htmlBody(new TextTemplate(html));
        }

        public Builder htmlBody(TextTemplate html) {
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
            if (Email.Field.FROM.match(fieldName, parser.getDeprecationHandler())) {
                builder.from(TextTemplate.parse(parser));
                validateEmailAddresses(builder.from);
            } else if (Email.Field.REPLY_TO.match(fieldName, parser.getDeprecationHandler())) {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    List<TextTemplate> templates = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        templates.add(TextTemplate.parse(parser));
                    }
                    builder.replyTo(templates.toArray(new TextTemplate[templates.size()]));
                } else {
                    builder.replyTo(TextTemplate.parse(parser));
                }
                validateEmailAddresses(builder.replyTo);
            } else if (Email.Field.TO.match(fieldName, parser.getDeprecationHandler())) {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    List<TextTemplate> templates = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        templates.add(TextTemplate.parse(parser));
                    }
                    builder.to(templates.toArray(new TextTemplate[templates.size()]));
                } else {
                    builder.to(TextTemplate.parse(parser));
                }
                validateEmailAddresses(builder.to);
            } else if (Email.Field.CC.match(fieldName, parser.getDeprecationHandler())) {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    List<TextTemplate> templates = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        templates.add(TextTemplate.parse(parser));
                    }
                    builder.cc(templates.toArray(new TextTemplate[templates.size()]));
                } else {
                    builder.cc(TextTemplate.parse(parser));
                }
                validateEmailAddresses(builder.cc);
            } else if (Email.Field.BCC.match(fieldName, parser.getDeprecationHandler())) {
                if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
                    List<TextTemplate> templates = new ArrayList<>();
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        templates.add(TextTemplate.parse(parser));
                    }
                    builder.bcc(templates.toArray(new TextTemplate[templates.size()]));
                } else {
                    builder.bcc(TextTemplate.parse(parser));
                }
                validateEmailAddresses(builder.bcc);
            } else if (Email.Field.PRIORITY.match(fieldName, parser.getDeprecationHandler())) {
                builder.priority(TextTemplate.parse(parser));
            } else if (Email.Field.SUBJECT.match(fieldName, parser.getDeprecationHandler())) {
                builder.subject(TextTemplate.parse(parser));
            } else if (Email.Field.BODY.match(fieldName, parser.getDeprecationHandler())) {
                if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                    builder.textBody(TextTemplate.parse(parser));
                } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                    XContentParser.Token token;
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (currentFieldName == null) {
                            throw new ElasticsearchParseException("could not parse email template. empty [{}] field", fieldName);
                        } else if (Email.Field.BODY_TEXT.match(currentFieldName, parser.getDeprecationHandler())) {
                            builder.textBody(TextTemplate.parse(parser));
                        } else if (Email.Field.BODY_HTML.match(currentFieldName, parser.getDeprecationHandler())) {
                            builder.htmlBody(TextTemplate.parse(parser));
                        } else {
                            throw new ElasticsearchParseException("could not parse email template. unknown field [{}.{}] field",
                                    fieldName, currentFieldName);
                        }
                    }
                }
            } else {
                return false;
            }
            return true;
        }

        /**
         * If this is a text template not using mustache
         * @param emails The list of email addresses to parse
         */
        static void validateEmailAddresses(TextTemplate ... emails) {
            for (TextTemplate emailTemplate : emails) {
                // no mustache, do validation
                if (emailTemplate.mayRequireCompilation() == false) {
                    String email = emailTemplate.getTemplate();
                    try {
                        for (Email.Address address : Email.AddressList.parse(email)) {
                            address.validate();
                        }
                    } catch (AddressException e) {
                        throw new ElasticsearchParseException("invalid email address [{}]", e, email);
                    }
                }
            }
        }

        public EmailTemplate parsedTemplate() {
            return builder.build();
        }
    }

}
