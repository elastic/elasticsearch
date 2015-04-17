/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.owasp.html.*;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * A profile of an email client, can be seen as a strategy to emulate a real world email client
 * (different clients potentially support different mime message structures)
 */
public enum Profile implements ToXContent {

    STANDARD() {

        @Override
        public String textBody(MimeMessage msg) throws IOException, MessagingException {
            MimeMultipart mixed = (MimeMultipart) msg.getContent();
            MimeMultipart related = null;
            for (int i = 0; i < mixed.getCount(); i++) {
                MimeBodyPart part = (MimeBodyPart) mixed.getBodyPart(i);

                if (part.getContentType().startsWith("multipart/related")) {
                    related = (MimeMultipart) part.getContent();
                    break;
                }
            }
            if (related == null) {
                throw new EmailException("could not extract body text from mime message");
            }

            MimeMultipart alternative = null;
            for (int i = 0; i < related.getCount(); i++) {
                MimeBodyPart part = (MimeBodyPart) related.getBodyPart(i);
                if (part.getContentType().startsWith("multipart/alternative")) {
                    alternative = (MimeMultipart) part.getContent();
                    break;
                }
            }
            if (alternative == null) {
                throw new EmailException("could not extract body text from mime message");
            }

            for (int i = 0; i < alternative.getCount(); i++) {
                MimeBodyPart part = (MimeBodyPart) alternative.getBodyPart(i);
                if (part.getContentType().startsWith("text/plain")) {
                    return (String) part.getContent();
                }
            }

            throw new EmailException("could not extract body text from mime message");
        }

        @Override
        public MimeMessage toMimeMessage(Email email, Session session) throws MessagingException {
            MimeMessage message = createCommon(email, session);

            MimeMultipart mixed = new MimeMultipart("mixed");
            message.setContent(mixed);

            MimeMultipart related = new MimeMultipart("related");
            mixed.addBodyPart(wrap(related, null));

            MimeMultipart alternative = new MimeMultipart("alternative");
            related.addBodyPart(wrap(alternative, "text/alternative"));

            MimeBodyPart text = new MimeBodyPart();
            if (email.textBody != null) {
                text.setText(email.textBody, Charsets.UTF_8.name());
            } else {
                text.setText("", Charsets.UTF_8.name());
            }
            alternative.addBodyPart(text);

            if (email.htmlBody != null) {
                MimeBodyPart html = new MimeBodyPart();
                String sanitizedHtml = sanitizeHtml(email.attachments, email.htmlBody);
                html.setText(sanitizedHtml, Charsets.UTF_8.name(), "html");
                alternative.addBodyPart(html);
            }

            if (!email.inlines.isEmpty()) {
                for (Inline inline : email.inlines.values()) {
                    related.addBodyPart(inline.bodyPart());
                }
            }

            if (!email.attachments.isEmpty()) {
                for (Attachment attachment : email.attachments.values()) {
                    mixed.addBodyPart(attachment.bodyPart());
                }
            }

            return message;
        }
    },

    OUTLOOK() {

        @Override
        public String textBody(MimeMessage msg) throws IOException, MessagingException {
            return STANDARD.textBody(msg);
        }

        @Override
        public MimeMessage toMimeMessage(Email email, Session session) throws MessagingException {
            return STANDARD.toMimeMessage(email, session);
        }
    },
    GMAIL() {

        @Override
        public String textBody(MimeMessage msg) throws IOException, MessagingException {
            return STANDARD.textBody(msg);
        }

        @Override
        public MimeMessage toMimeMessage(Email email, Session session) throws MessagingException {
            return STANDARD.toMimeMessage(email, session);
        }
    },
    MAC() {

        @Override
        public String textBody(MimeMessage msg) throws IOException, MessagingException {
            return STANDARD.textBody(msg);
        }

        @Override
        public MimeMessage toMimeMessage(Email email, Session session) throws MessagingException {
            return STANDARD.toMimeMessage(email, session);
        }
    };

    static final String MESSAGE_ID_HEADER = "Message-ID";

    public abstract MimeMessage toMimeMessage(Email email, Session session) throws MessagingException ;

    public abstract String textBody(MimeMessage msg) throws IOException, MessagingException;

    public static Profile resolve(String name) {
        Profile profile = resolve(name, null);
        if (profile == null) {
            throw new EmailSettingsException("unsupported email profile [" + name + "]");
        }
        return profile;
    }

    public static Profile resolve(String name, Profile defaultProfile) {
        if (name == null) {
            return defaultProfile;
        }
        switch (name.toLowerCase(Locale.ROOT)) {
            case "std":
            case "standard":    return STANDARD;
            case "outlook":     return OUTLOOK;
            case "gmail":       return GMAIL;
            case "mac":         return MAC;
            default:
                return defaultProfile;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(name().toLowerCase(Locale.ROOT));
    }

    static MimeMessage createCommon(Email email, Session session) throws MessagingException {
        MimeMessage message = new MimeMessage(session);
        message.setHeader(MESSAGE_ID_HEADER, email.id);
        if (email.from != null) {
            message.setFrom(email.from);
        }
        if (email.replyTo != null) {
            message.setReplyTo(email.replyTo.toArray());
        }
        if (email.priority != null) {
            email.priority.applyTo(message);
        }
        message.setSentDate(email.sentDate.toDate());
        message.setRecipients(Message.RecipientType.TO, email.to.toArray());
        if (email.cc != null) {
            message.setRecipients(Message.RecipientType.CC, email.cc.toArray());
        }
        if (email.bcc != null) {
            message.setRecipients(Message.RecipientType.BCC, email.bcc.toArray());
        }
        if (email.subject != null) {
            message.setSubject(email.subject, Charsets.UTF_8.name());
        } else {
            message.setSubject("", Charsets.UTF_8.name());
        }

        return message;
    }

    static MimeBodyPart wrap(MimeMultipart multipart, String contentType) throws MessagingException {
        MimeBodyPart part = new MimeBodyPart();
        if (contentType == null) {
            part.setContent(multipart);
        } else {
            part.setContent(multipart, contentType);
        }
        return part;
    }

    static String sanitizeHtml(final ImmutableMap<String, Attachment> attachments, String html){
        ElementPolicy onlyCIDImgPolicy = new AttachementVerifyElementPolicy(attachments);
        PolicyFactory policy = Sanitizers.FORMATTING
                .and(new HtmlPolicyBuilder()
                        .allowElements("img", "table", "tr", "td", "style", "body", "head")
                        .allowAttributes("src").onElements("img")
                        .allowAttributes("class").onElements("style")
                        .allowUrlProtocols("cid")
                        .allowCommonInlineFormattingElements()
                        .allowElements(onlyCIDImgPolicy, "img")
                        .allowStyling(CssSchema.DEFAULT)
                        .toFactory())
                .and(Sanitizers.LINKS)
                .and(Sanitizers.BLOCKS);
        return policy.sanitize(html);
    }



    private static class AttachementVerifyElementPolicy implements ElementPolicy {

        private final ImmutableMap<String, Attachment> attachments;

        AttachementVerifyElementPolicy(ImmutableMap<String, Attachment> attchments) {
            this.attachments = attchments;
        }

        @Nullable
        @Override
        public String apply(@ParametersAreNonnullByDefault String elementName, @ParametersAreNonnullByDefault List<String> attrs) {
            if (attrs.size() == 0) {
                return elementName;
            }
            for (int i = 0; i < attrs.size(); ++i) {
                if(attrs.get(i).equals("src") && i < attrs.size() - 1) {
                    String srcValue = attrs.get(i+1);
                    if (!srcValue.startsWith("cid:")) {
                        return null; //Disallow anything other than content ids
                    }
                    String contentId = srcValue.substring(4);
                    if (attachments.containsKey(contentId)) {
                        return elementName;
                    } else {
                        return null; //This cid wasn't found
                    }
                }
            }
            return elementName;
        }
    }
}
