/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email.service;

import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.IOException;
import java.util.Date;
import java.util.Locale;

/**
 * A profile of an email client, can be seen as a strategy to emulate a real world email client
 * (different clients potentially support different mime message structures)
 */
public enum Profile implements ToXContent {

    STANDARD() {
        @Override
        public MimeMessage toMimeMessage(Email email, Session session) throws MessagingException {
            MimeMessage message = createCommon(email, session);

            MimeMultipart mixed = new MimeMultipart("mixed");

            MimeMultipart related = new MimeMultipart("related");
            mixed.addBodyPart(wrap(related, null));

            MimeMultipart alternative = new MimeMultipart("alternative");
            related.addBodyPart(wrap(alternative, "text/alternative"));

            MimeBodyPart text = new MimeBodyPart();
            text.setText(email.textBody, Charsets.UTF_8.name());
            alternative.addBodyPart(text);

            if (email.htmlBody != null) {
                MimeBodyPart html = new MimeBodyPart();
                text.setText(email.textBody, Charsets.UTF_8.name(), "html");
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
        public MimeMessage toMimeMessage(Email email, Session session) throws MessagingException {
            return STANDARD.toMimeMessage(email, session);
        }
    },
    GMAIL() {
        @Override
        public MimeMessage toMimeMessage(Email email, Session session) throws MessagingException {
            return STANDARD.toMimeMessage(email, session);
        }
    },
    MAC() {
        @Override
        public MimeMessage toMimeMessage(Email email, Session session) throws MessagingException {
            return STANDARD.toMimeMessage(email, session);
        }
    };

    static final String MESSAGE_ID_HEADER = "Message-ID";

    public abstract MimeMessage toMimeMessage(Email email, Session session) throws MessagingException ;

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
        Date sentDate = email.sentDate != null ? email.sentDate.toDate() : new Date();
        message.setSentDate(sentDate);

        message.setRecipients(Message.RecipientType.TO, email.to.toArray());
        message.setRecipients(Message.RecipientType.CC, email.cc.toArray());
        message.setRecipients(Message.RecipientType.BCC, email.bcc.toArray());

        message.setSubject(email.subject, Charsets.UTF_8.name());

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
