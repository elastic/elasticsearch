/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Locale;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

/**
 * A profile of an email client, can be seen as a strategy to emulate a real world email client
 * (different clients potentially support different mime message structures)
 */
public enum Profile {

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
                throw new IllegalStateException(
                    "could not extract body text from mime message using [standard] profile. could not find "
                        + "part content type with [multipart/related]"
                );
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
                throw new IllegalStateException(
                    "could not extract body text from mime message using [standard] profile. could not find "
                        + "part content type with [multipart/alternative]"
                );
            }

            for (int i = 0; i < alternative.getCount(); i++) {
                MimeBodyPart part = (MimeBodyPart) alternative.getBodyPart(i);
                if (part.getContentType().startsWith("text/plain")) {
                    return (String) part.getContent();
                }
            }

            throw new IllegalStateException("could not extract body text from mime message using [standard] profile");
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
                text.setText(email.textBody, StandardCharsets.UTF_8.name());
            } else {
                text.setText("", StandardCharsets.UTF_8.name());
            }
            alternative.addBodyPart(text);

            if (email.htmlBody != null) {
                MimeBodyPart html = new MimeBodyPart();
                html.setText(email.htmlBody, StandardCharsets.UTF_8.name(), "html");
                alternative.addBodyPart(html);
            }

            if (email.attachments.isEmpty() == false) {
                for (Attachment attachment : email.attachments.values()) {
                    if (attachment.isInline()) {
                        related.addBodyPart(attachment.bodyPart());
                    } else {
                        mixed.addBodyPart(attachment.bodyPart());
                    }
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

    public abstract MimeMessage toMimeMessage(Email email, Session session) throws MessagingException;

    public abstract String textBody(MimeMessage msg) throws IOException, MessagingException;

    public static Profile resolve(String name) {
        Profile profile = resolve(name, null);
        if (profile == null) {
            throw new IllegalArgumentException("[" + name + "] is an unknown email profile");
        }
        return profile;
    }

    public static Profile resolve(String name, Profile defaultProfile) {
        if (name == null) {
            return defaultProfile;
        }
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "std", "standard" -> STANDARD;
            case "outlook" -> OUTLOOK;
            case "gmail" -> GMAIL;
            case "mac" -> MAC;
            default -> defaultProfile;
        };
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
        message.setSentDate(Date.from(email.sentDate.toInstant()));
        message.setRecipients(Message.RecipientType.TO, email.to.toArray());
        if (email.cc != null) {
            message.setRecipients(Message.RecipientType.CC, email.cc.toArray());
        }
        if (email.bcc != null) {
            message.setRecipients(Message.RecipientType.BCC, email.bcc.toArray());
        }
        if (email.subject != null) {
            message.setSubject(email.subject, StandardCharsets.UTF_8.name());
        } else {
            message.setSubject("", StandardCharsets.UTF_8.name());
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
}
