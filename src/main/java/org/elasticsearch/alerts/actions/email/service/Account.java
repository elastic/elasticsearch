/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions.email.service;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

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

    private final ESLogger logger;
    private final Config config;
    private final Session session;

    Account(Config config, ESLogger logger) {
        this.config = config;
        this.logger = logger;
        session = config.createSession();
    }

    public String name() {
        return config.name;
    }

    public void send(Email email, Authentication auth, Profile profile) throws MessagingException {

        // applying the defaults on missing emails fields
        email = config.defaults.apply(email);

        Transport transport = session.getTransport(SMTP_PROTOCOL);
        String user = auth != null ? auth.username() : null;
        if (user == null) {
            user = config.smtp.user;
            if (user == null) {
                user = InternetAddress.getLocalAddress(session).getAddress();
            }
        }
        String password = auth != null ? auth.password() : null;
        if (password == null) {
            password = config.smtp.password;
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
            if (transport != null) {
                try {
                    transport.close();
                } catch (MessagingException me) {
                    logger.error("failed to close email transport for account [" + config.name + "]");
                }
            }
        }
    }

    static class Config {

        static final String SMTP_SETTINGS_PREFIX = "mail.smtp.";

        private final String name;
        private final Profile profile;
        private final Smtp smtp;
        private final EmailDefaults defaults;

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

            private final String host;
            private final int port;
            private final String user;
            private final String password;
            private final Properties properties;

            public Smtp(Settings settings) {
                host = settings.get("host");
                port = settings.getAsInt("port", settings.getAsInt("localport", 25));
                user = settings.get("user", settings.get("from", settings.get("local_address", null)));
                password = settings.get("password", null);
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
         * needed on each alert (e.g. if all the emails are always sent to the same recipients
         * one could set those here and leave them out on the alert definition).
         */
        class EmailDefaults {

            final Email.Address from;
            final Email.AddressList replyTo;
            final Email.Priority priority;
            final Email.AddressList to;
            final Email.AddressList cc;
            final Email.AddressList bcc;
            final String subject;

            public EmailDefaults(Settings settings) {
                from = Email.Address.parse(settings, Email.FROM_FIELD.getPreferredName());
                replyTo = Email.AddressList.parse(settings, Email.REPLY_TO_FIELD.getPreferredName());
                priority = Email.Priority.parse(settings, Email.PRIORITY_FIELD.getPreferredName());
                to = Email.AddressList.parse(settings, Email.TO_FIELD.getPreferredName());
                cc = Email.AddressList.parse(settings, Email.CC_FIELD.getPreferredName());
                bcc = Email.AddressList.parse(settings, Email.BCC_FIELD.getPreferredName());
                subject = settings.get(Email.SUBJECT_FIELD.getPreferredName());
            }

            Email apply(Email email) {
                return Email.builder()
                        .from(from)
                        .replyTo(replyTo)
                        .priority(priority)
                        .to(to)
                        .cc(cc)
                        .bcc(bcc)
                        .subject(subject)
                        .copyFrom(email)
                        .build();
            }
        }
    }
}
