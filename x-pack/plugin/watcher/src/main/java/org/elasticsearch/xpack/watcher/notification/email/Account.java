/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.email;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoService;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.activation.CommandMap;
import javax.activation.MailcapCommandMap;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import static org.elasticsearch.xpack.core.watcher.WatcherField.EMAIL_NOTIFICATION_SSL_PREFIX;

public class Account {

    static final String SMTP_PROTOCOL = "smtp";
    public static final Setting<SecureString> SECURE_PASSWORD_SETTING = SecureSetting.secureString("secure_password", null);

    static {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            // required as java doesn't always find the correct mailcap to properly handle mime types
            final MailcapCommandMap mailcap = (MailcapCommandMap) CommandMap.getDefaultCommandMap();
            mailcap.addMailcap("text/html;; x-java-content-handler=com.sun.mail.handlers.text_html");
            mailcap.addMailcap("text/xml;; x-java-content-handler=com.sun.mail.handlers.text_xml");
            mailcap.addMailcap("text/plain;; x-java-content-handler=com.sun.mail.handlers.text_plain");
            mailcap.addMailcap("multipart/*;; x-java-content-handler=com.sun.mail.handlers.multipart_mixed");
            mailcap.addMailcap("message/rfc822;; x-java-content-handler=com.sun.mail.handlers.message_rfc822");
            CommandMap.setDefaultCommandMap(mailcap);
            return null;
        });
    }

    // exists only to allow ensuring class is initialized
    public static void init() {}

    static final Settings DEFAULT_SMTP_TIMEOUT_SETTINGS = Settings.builder()
        .put("connection_timeout", TimeValue.timeValueMinutes(2))
        .put("write_timeout", TimeValue.timeValueMinutes(2))
        .put("timeout", TimeValue.timeValueMinutes(2))
        .build();

    private final Config config;
    private final CryptoService cryptoService;
    private final Logger logger;
    private final Session session;

    Account(Config config, CryptoService cryptoService, Logger logger) {
        this.config = config;
        this.cryptoService = cryptoService;
        this.logger = logger;
        session = config.createSession();
    }

    public String name() {
        return config.name;
    }

    Config getConfig() {
        return config;
    }

    public Email send(Email email, Authentication auth, Profile profile) throws MessagingException {

        // applying the defaults on missing emails fields
        email = config.defaults.apply(email);

        if (email.to == null) {
            throw new SettingsException("missing required email [to] field");
        }

        Transport transport = session.getTransport(SMTP_PROTOCOL);

        String user = auth != null ? auth.user() : config.smtp.user;
        if (user == null) {
            InternetAddress localAddress = InternetAddress.getLocalAddress(session);
            // null check needed, because if the local host does not resolve, this may be null
            // this can happen in wrongly setup linux distributions
            if (localAddress != null) {
                user = localAddress.getAddress();
            }
        }

        String password = null;
        if (auth != null && auth.password() != null) {
            password = new String(auth.password().text(cryptoService));
        } else if (config.smtp.password != null) {
            password = new String(config.smtp.password.getChars());
        }

        if (profile == null) {
            profile = config.profile;
        }

        executeConnect(transport, user, password);
        ClassLoader contextClassLoader = null;
        try {
            MimeMessage message = profile.toMimeMessage(email, session);
            String mid = message.getMessageID();
            message.saveChanges();
            if (mid != null) {
                // saveChanges may rewrite/remove the message id, so
                // we need to add it back
                message.setHeader(Profile.MESSAGE_ID_HEADER, mid);
            }

            SecurityManager sm = System.getSecurityManager();
            if (sm != null) {
                // unprivileged code such as scripts do not have SpecialPermission
                sm.checkPermission(new SpecialPermission());
            }
            contextClassLoader = AccessController.doPrivileged(
                (PrivilegedAction<ClassLoader>) () -> Thread.currentThread().getContextClassLoader()
            );
            // if we cannot get the context class loader, changing does not make sense, as we run into the danger of not being able to
            // change it back
            if (contextClassLoader != null) {
                setContextClassLoader(this.getClass().getClassLoader());
            }
            transport.sendMessage(message, message.getAllRecipients());
        } finally {
            try {
                transport.close();
            } catch (MessagingException me) {
                logger.error("failed to close email transport for account [{}]", config.name);
            }
            if (contextClassLoader != null) {
                setContextClassLoader(contextClassLoader);
            }
        }
        return email;
    }

    private void executeConnect(Transport transport, String user, String password) throws MessagingException {
        SpecialPermission.check();
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                transport.connect(config.smtp.host, config.smtp.port, user, password);
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw (MessagingException) e.getCause();
        }
    }

    private void setContextClassLoader(final ClassLoader classLoader) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            Thread.currentThread().setContextClassLoader(classLoader);
            return null;
        });
    }

    static class Config {

        static final String SMTP_SETTINGS_PREFIX = "mail.smtp.";

        final String name;
        final Profile profile;
        final Smtp smtp;
        final EmailDefaults defaults;

        Config(String name, Settings settings, @Nullable SSLSocketFactory sslSocketFactory, Logger logger) {
            this.name = name;
            profile = Profile.resolve(settings.get("profile"), Profile.STANDARD);
            defaults = new EmailDefaults(name, settings.getAsSettings("email_defaults"));
            smtp = new Smtp(settings.getAsSettings(SMTP_PROTOCOL));
            if (smtp.host == null) {
                String msg = "missing required email account setting for account [" + name + "]. 'smtp.host' must be configured";
                throw new SettingsException(msg);
            }
            if (sslSocketFactory != null) {
                String sslKeys = smtp.properties.keySet()
                    .stream()
                    .map(String::valueOf)
                    .filter(key -> key.startsWith("mail.smtp.ssl."))
                    .collect(Collectors.joining(","));
                if (sslKeys.isEmpty() == false) {
                    logger.warn(
                        "The SMTP SSL settings [{}] that are configured for Account [{}]"
                            + " will be ignored due to the notification SSL settings in [{}]",
                        sslKeys,
                        name,
                        EMAIL_NOTIFICATION_SSL_PREFIX
                    );
                }
                smtp.setSocketFactory(sslSocketFactory);
            }
        }

        public Session createSession() {
            return Session.getInstance(smtp.properties);
        }

        static class Smtp {

            final String host;
            final int port;
            final String user;
            final SecureString password;
            final Properties properties;

            Smtp(Settings settings) {
                host = settings.get("host", settings.get("localaddress", settings.get("local_address")));

                port = settings.getAsInt("port", settings.getAsInt("localport", settings.getAsInt("local_port", 25)));
                user = settings.get("user", settings.get("from", null));
                password = getSecureSetting(settings, SECURE_PASSWORD_SETTING);
                // password = passStr != null ? passStr.toCharArray() : null;
                properties = loadSmtpProperties(settings);
            }

            /**
             * Finds a setting, and then a secure setting if the setting is null, or returns null if one does not exist. This differs
             * from other getSetting calls in that it allows for null whereas the other methods throw an exception.
             * <p>
             * Note: if your setting was not previously secure, than the string reference that is in the setting object is still
             * insecure. This is only constructing a new SecureString with the char[] of the insecure setting.
             */
            private static SecureString getSecureSetting(Settings settings, Setting<SecureString> secureSetting) {
                SecureString secureString = secureSetting.get(settings);
                if (secureString != null && secureString.length() > 0) {
                    return secureString;
                } else {
                    return null;
                }
            }

            /**
             * loads the standard Java Mail properties as settings from the given account settings.
             * The standard settings are not that readable, therefore we enabled the user to configure
             * those in a readable way... this method first loads the smtp settings (which corresponds to
             * all Java Mail {@code mail.smtp.*} settings), and then replaces the readable keys to the official
             * "unreadable" keys. We'll then use these settings when crea
             */
            static Properties loadSmtpProperties(Settings settings) {
                Settings.Builder builder = Settings.builder().put(DEFAULT_SMTP_TIMEOUT_SETTINGS).put(settings);
                replaceTimeValue(builder, "connection_timeout", "connectiontimeout");
                replaceTimeValue(builder, "write_timeout", "writetimeout");
                replaceTimeValue(builder, "timeout", "timeout");

                replace(builder, "local_address", "localaddress");
                replace(builder, "local_port", "localport");
                replace(builder, "send_partial", "sendpartial");
                replace(builder, "wait_on_quit", "quitwait");

                settings = builder.build();
                Properties props = new Properties();
                // Secure strings can not be retreived out of a settings object and should be handled differently
                Set<String> insecureSettings = settings.filter(s -> s.startsWith("secure_") == false).keySet();
                for (String key : insecureSettings) {
                    props.setProperty(SMTP_SETTINGS_PREFIX + key, settings.get(key));
                }
                return props;
            }

            static void replace(Settings.Builder settings, String currentKey, String newKey) {
                String value = settings.remove(currentKey);
                if (value != null) {
                    settings.put(newKey, value);
                }
            }

            static void replaceTimeValue(Settings.Builder settings, String currentKey, String newKey) {
                String value = settings.remove(currentKey);
                if (value != null) {
                    settings.put(newKey, TimeValue.parseTimeValue(value, currentKey).millis());
                }
            }

            public void setSocketFactory(SocketFactory socketFactory) {
                this.properties.put("mail.smtp.ssl.socketFactory", socketFactory);
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

            EmailDefaults(String accountName, Settings settings) {
                try {
                    from = Email.Address.parse(settings, Email.Field.FROM.getPreferredName());
                    replyTo = Email.AddressList.parse(settings, Email.Field.REPLY_TO.getPreferredName());
                    priority = Email.Priority.parse(settings, Email.Field.PRIORITY.getPreferredName());
                    to = Email.AddressList.parse(settings, Email.Field.TO.getPreferredName());
                    cc = Email.AddressList.parse(settings, Email.Field.CC.getPreferredName());
                    bcc = Email.AddressList.parse(settings, Email.Field.BCC.getPreferredName());
                    subject = settings.get(Email.Field.SUBJECT.getPreferredName());
                } catch (IllegalArgumentException iae) {
                    throw new SettingsException("invalid email defaults in email account settings [" + accountName + "]", iae);
                }
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
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                EmailDefaults that = (EmailDefaults) o;
                return Objects.equals(bcc, that.bcc)
                    && Objects.equals(cc, that.cc)
                    && Objects.equals(from, that.from)
                    && priority == that.priority
                    && Objects.equals(replyTo, that.replyTo)
                    && Objects.equals(subject, that.subject)
                    && Objects.equals(to, that.to);
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
