/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.ConfigurableComponentListener;
import org.elasticsearch.alerts.ConfigurationService;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.util.*;

public class SmtpAlertActionFactory implements AlertActionFactory, ConfigurableComponentListener {

    private static final String PORT_SETTING = "alerts.action.email.server.port";
    private static final String SERVER_SETTING = "alerts.action.email.server.name";
    private static final String FROM_SETTING = "alerts.action.email.from.address";
    private static final String PASSWD_SETTING = "alerts.action.email.from.passwd";
    private static final String USERNAME_SETTING = "alerts.action.email.from.username";

    private static final String ALERT_NAME_VARIABLE_NAME = "alert_name";
    private static final String RESPONSE_VARIABLE_NAME = "response";

    private static final String DEFAULT_SUBJECT = "Elasticsearch Alert {{alert_name}} triggered";
    private static final String DEFAULT_MESSAGE = "{{alert_name}} triggered with {{response.hits.total}} results";

    private final ConfigurationService configurationService;
    private final ScriptService scriptService;

    private volatile Settings settings;

    public SmtpAlertActionFactory(ConfigurationService configurationService, ScriptService scriptService) {
        this.configurationService = configurationService;
        this.scriptService = scriptService;
    }

    @Override
    public AlertAction createAction(XContentParser parser) throws IOException {
        if (settings == null) {
            settings = configurationService.getConfig();
            configurationService.registerListener(this);
        }

        String messageTemplate = DEFAULT_MESSAGE;
        String subjectTemplate = DEFAULT_SUBJECT;

        List<String> addresses = new ArrayList<>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case "subject":
                        subjectTemplate = parser.text();
                        break;
                    case "message":
                        messageTemplate = parser.text();
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                switch (currentFieldName) {
                    case "addresses":
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            addresses.add(parser.text());
                        }
                        break;
                    default:
                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new ElasticsearchIllegalArgumentException("Unexpected token [" + token + "]");
            }
        }
        return new SmtpAlertAction(subjectTemplate, messageTemplate, addresses.toArray(new String[addresses.size()]));
    }

    @Override
    public boolean doAction(AlertAction action, Alert alert, TriggerResult result) {
        if (!(action instanceof SmtpAlertAction)) {
            throw new ElasticsearchIllegalStateException("Bad action [" + action.getClass() + "] passed to EmailAlertActionFactory expected [" + SmtpAlertAction.class + "]");
        }

        if (settings == null) {
            throw new ElasticsearchException("No settings loaded for Smtp (email)");
        }

        SmtpAlertAction smtpAlertAction = (SmtpAlertAction)action;

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", settings.get(SERVER_SETTING, "smtp.gmail.com"));
        props.put("mail.smtp.port", settings.getAsInt(PORT_SETTING, 587));

        final Session session;
        if (settings.get(PASSWD_SETTING) != null) {
            session = Session.getInstance(props,
                    new javax.mail.Authenticator() {
                        protected PasswordAuthentication getPasswordAuthentication() {
                            return new PasswordAuthentication(
                                    settings.get(USERNAME_SETTING) == null ? settings.get(FROM_SETTING) : settings.get(USERNAME_SETTING),
                                    settings.get(PASSWD_SETTING));
                        }
                    });
        } else {
            session = Session.getDefaultInstance(props);
        }

        Message message = new MimeMessage(session);
        try {
            message.setFrom(new InternetAddress(settings.get(FROM_SETTING)));
            message.setRecipients(Message.RecipientType.TO,
                    smtpAlertAction.getEmailAddresses().toArray(new Address[1]));

            if (smtpAlertAction.getSubjectTemplate() != null) {
                message.setSubject(renderTemplate(smtpAlertAction.getSubjectTemplate(), alert, result, scriptService));
            } else {
                throw new ElasticsearchException("Subject Template not found");
            }

            if (smtpAlertAction.getMessageTemplate() != null) {
                message.setText(renderTemplate(smtpAlertAction.getMessageTemplate(), alert, result, scriptService));
            } else {
                throw new ElasticsearchException("Email Message Template not found");
            }
            Transport.send(message);
        } catch (Exception e){
            throw new ElasticsearchException("Failed to send mail", e);
        }
        return true;
    }

    @Override
    public void receiveConfigurationUpdate(Settings settings) {
        this.settings = settings;
    }

    public static String renderTemplate(String template, Alert alert, TriggerResult result, ScriptService scriptService) {
        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put(ALERT_NAME_VARIABLE_NAME, alert.getAlertName());
        templateParams.put(RESPONSE_VARIABLE_NAME, result.getActionResponse());
        ExecutableScript script = scriptService.executable("mustache", template, ScriptService.ScriptType.INLINE, templateParams);
        return ((BytesReference) script.run()).toUtf8();
    }

}
