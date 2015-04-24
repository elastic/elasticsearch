/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.email.service;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.support.secret.SecretService;

import javax.activation.CommandMap;
import javax.activation.MailcapCommandMap;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class Account {

    static final String SMTP_PROTOCOL = "smtp";

    static {
        // required as java doesn't always find the correct mailcap to properly handle mime types
        MailcapCommandMap mailcap = (MailcapCommandMap) CommandMap.getDefaultCommandMap();
        mailcap.addMailcap("text/html;; x-java-content-handler=com.sun.mail.handlers.text_html");
        mailcap.addMailcap("text/xml;; x-java-content-handler=com.sun.mail.handlers.text_xml");
        mailcap.addMailcap("text/plain;; x-java-content-handler=com.sun.mail.handlers.text_plain");
        mailcap.addMailcap("multipart/*;; x-java-content-handler=com.sun.mail.handlers.multipart_mixed");
        mailcap.addMailcap("message/rfc822;; x-java-content-handler=com.sun.mail.handlers.message_rfc822");
        CommandMap.setDefaultCommandMap(mailcap);
    }

    private final Config config;
    private final SecretService secretService;
    private final ESLogger logger;
    private final Session session;

    Account(Config config, SecretService secretService, ESLogger logger) {
        this.config = config;
        this.secretService = secretService;
        this.logger = logger;
        session = config.createSession();
    }

    public String name() {
        return config.name;
    }

    public Email send(Email email, Authentication auth, Profile profile) throws MessagingException {

        // applying the defaults on missing emails fields
        email = config.defaults.apply(email);

        if (email.to == null) {
            throw new EmailException("email must have [to] recipient");
        }

        Transport transport = session.getTransport(SMTP_PROTOCOL);

        String user = auth != null ? auth.user() : null;
        if (user == null) {
            user = config.smtp.user;
            if (user == null) {
                user = InternetAddress.getLocalAddress(session).getAddress();
            }
        }

        String password = null;
        if (auth != null && auth.password() != null) {
            password = new String(auth.password().text(secretService));
        } else if (config.smtp.password != null) {
            password = new String(config.smtp.password);
        }

        if (profile == null) {
            profile = config.profile;
        }

        transport.connect(config.smtp.host, config.smtp.port, user, password);
        try {

            MimeMessage message = profile.toMimeMessage(email, session);
            String mid = message.getMessageID();
            message.saveChanges();
            if (mid != null) {
                // saveChanges may rewrite/remove the message id, so
                // we need to add it back
                message.setHeader(Profile.MESSAGE_ID_HEADER, mid);
            }
            transport.sendMessage(message, message.getAllRecipients());
        } finally {
            try {
                transport.close();
            } catch (MessagingException me) {
                logger.error("failed to close email transport for account [" + config.name + "]");
            }
        }
        return email;
    }

    static class Config {

        static final String SMTP_SETTINGS_PREFIX = "mail.smtp.";

        final String name;
        final Profile profile;
        final Smtp smtp;
        final EmailDefaults defaults;

        public Config(String name, Settings settings) {
            this.name = name;
            profile = Profile.resolve(settings.get("profile"), Profile.STANDARD);
            defaults = new EmailDefaults(settings.getAsSettings("email_defaults"));
            smtp = new Smtp(settings.getAsSettings(SMTP_PROTOCOL));
            if (smtp.host == null) {
                throw new EmailSettingsException("missing required email account setting for account [" + name + "]. 'smtp.host' must be configured");
            }
        }

        public Session createSession() {
            return Session.getInstance(smtp.properties);
        }

        static class Smtp {

            final String host;
            final int port;
            final String user;
            final char[] password;
            final Properties properties;

            public Smtp(Settings settings) {
                host = settings.get("host", settings.get("localaddress", settings.get("local_address")));
                port = settings.getAsInt("port", settings.getAsInt("localport", settings.getAsInt("local_port", 25)));
                user = settings.get("user", settings.get("from", null));
                String passStr = settings.get("password", null);
                password = passStr != null ? passStr.toCharArray() : null;
                properties = loadSmtpProperties(settings);
            }

            /**
             * loads the standard Java Mail properties as settings from the given account settings.
             * The standard settings are not that readable, therefore we enabled the user to configure
             * those in a readable way... this method first loads the smtp settings (which corresponds to
             * all Java Mail {@code mail.smtp.*} settings), and then replaces the readable keys to the official
             * "unreadable" keys. We'll then use these settings when crea
             */
            static Properties loadSmtpProperties(Settings settings) {
                ImmutableSettings.Builder builder = ImmutableSettings.builder().put(settings);
                replace(builder, "connection_timeout", "connectiontimeout");
                replace(builder, "write_timeout", "writetimeout");
                replace(builder, "local_address", "localaddress");
                replace(builder, "local_port", "localport");
                replace(builder, "allow_8bitmime", "allow8bitmime");
                replace(builder, "send_partial", "sendpartial");
                replace(builder, "sasl.authorization_id", "sasl.authorizationid");
                replace(builder, "sasl.use_canonical_hostname", "sasl.usecanonicalhostname");
                replace(builder, "wait_on_quit", "quitwait");
                replace(builder, "report_success", "reportsuccess");
                replace(builder, "mail_extension", "mailextension");
                replace(builder, "use_rset", "userset");
                settings = builder.build();
                Properties props = new Properties();
                for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                    props.setProperty(SMTP_SETTINGS_PREFIX + entry.getKey(), entry.getValue());
                }
                return props;
            }

            static void replace(ImmutableSettings.Builder settings, String currentKey, String newKey) {
                String value = settings.remove(currentKey);
                if (value != null) {
                    settings.put(newKey, value);
                }
            }

        }

        /**
         * holds email fields that can be configured on the account. These fields
         * will hold the default values for missing fields in email messages. Having
         * the ability to create these default can substantially reduced the configuration
         * needed on each watch (e.g. if all the emails are always sent to the same recipients
         * one could set those here and leave them out on the watch definition).
         */
        static class EmailDefaults {

            final Email.Address from;
            final Email.AddressList replyTo;
            final Email.Priority priority;
            final Email.AddressList to;
            final Email.AddressList cc;
            final Email.AddressList bcc;
            final String subject;

            public EmailDefaults(Settings settings) {
                from = Email.Address.parse(settings, Email.Field.FROM.getPreferredName());
                replyTo = Email.AddressList.parse(settings, Email.Field.REPLY_TO.getPreferredName());
                priority = Email.Priority.parse(settings, Email.Field.PRIORITY.getPreferredName());
                to = Email.AddressList.parse(settings, Email.Field.TO.getPreferredName());
                cc = Email.AddressList.parse(settings, Email.Field.CC.getPreferredName());
                bcc = Email.AddressList.parse(settings, Email.Field.BCC.getPreferredName());
                subject = settings.get(Email.Field.SUBJECT.getPreferredName());
            }

            Email apply(Email email) {
                Email.Builder builder = Email.builder().copyFrom(email);
                if (email.from == null) {
                    builder.from(from);
                }
                if (email.replyTo == null) {
                    builder.replyTo(replyTo);
                }
                if (email.priority == null) {
                    builder.priority(priority);
                }
                if (email.to == null) {
                    builder.to(to);
                }
                if (email.cc == null) {
                    builder.cc(cc);
                }
                if (email.bcc == null) {
                    builder.bcc(bcc);
                }
                if (email.subject == null) {
                    builder.subject(subject);
                }
                return builder.build();
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                EmailDefaults that = (EmailDefaults) o;

                if (bcc != null ? !bcc.equals(that.bcc) : that.bcc != null) return false;
                if (cc != null ? !cc.equals(that.cc) : that.cc != null) return false;
                if (from != null ? !from.equals(that.from) : that.from != null) return false;
                if (priority != that.priority) return false;
                if (replyTo != null ? !replyTo.equals(that.replyTo) : that.replyTo != null) return false;
                if (subject != null ? !subject.equals(that.subject) : that.subject != null) return false;
                if (to != null ? !to.equals(that.to) : that.to != null) return false;

                return true;
            }

            @Override
            public int hashCode() {
                int result = from != null ? from.hashCode() : 0;
                result = 31 * result + (replyTo != null ? replyTo.hashCode() : 0);
                result = 31 * result + (priority != null ? priority.hashCode() : 0);
                result = 31 * result + (to != null ? to.hashCode() : 0);
                result = 31 * result + (cc != null ? cc.hashCode() : 0);
                result = 31 * result + (bcc != null ? bcc.hashCode() : 0);
                result = 31 * result + (subject != null ? subject.hashCode() : 0);
                return result;
            }
        }
    }
}
