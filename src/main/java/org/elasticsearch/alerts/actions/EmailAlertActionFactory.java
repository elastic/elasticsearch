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
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class EmailAlertActionFactory implements AlertActionFactory {

    // TODO: Move to factory and make configurable
    private final int port = 587;
    private final String server = "smtp.gmail.com";
    private final String from = "esalertingtest@gmail.com";
    private final String  passwd = "elasticsearchforthewin";


    @Override
    public AlertAction createAction(XContentParser parser) throws IOException {
        String display = null;
        List<String> addresses = new ArrayList<>();

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case "display":
                        display = parser.text();
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
        return new EmailAlertAction(display, addresses.toArray(new String[addresses.size()]));
    }

    @Override
    public boolean doAction(AlertAction action, Alert alert, TriggerResult result) {
        if (!(action instanceof EmailAlertAction)) {
            throw new ElasticsearchIllegalStateException("Bad action [" + action.getClass() + "] passed to EmailAlertActionFactory expected [" + EmailAlertAction.class + "]");
        }
        EmailAlertAction emailAlertAction = (EmailAlertAction)action;

        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", server);
        props.put("mail.smtp.port", port);
        Session session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(from, passwd);
                    }
                });
        Message message = new MimeMessage(session);
        try {
            message.setFrom(new InternetAddress(from));
            message.setRecipients(Message.RecipientType.TO,
                    emailAlertAction.getEmailAddresses().toArray(new Address[1]));
            message.setSubject("Elasticsearch Alert " + alert.getAlertName() + " triggered");
            StringBuilder output = new StringBuilder();
            output.append("The following query triggered because ").append(result.getTrigger().toString()).append("\n");
            Object totalHits = XContentMapValues.extractValue("hits.total", result.getResponse());
            output.append("The total number of hits returned : ").append(totalHits).append("\n");
            output.append("For query : ").append(result.getRequest());
            output.append("\n");
            output.append("Indices : ");
            for (String index : result.getRequest().indices()) {
                output.append(index);
                output.append("/");
            }
            output.append("\n");
            output.append("\n");

            if (emailAlertAction.getDisplayField() != null) {
                List<Map<String, Object>> hits = (List<Map<String, Object>>) XContentMapValues.extractValue("hits.hits", result.getResponse());
                for (Map<String, Object> hit : hits) {
                    Map<String, Object> _source = (Map<String, Object>) hit.get("_source");
                    if (_source.containsKey(emailAlertAction.getDisplayField())) {
                        output.append(_source.get(emailAlertAction.getDisplayField()).toString());
                    } else {
                        output.append(_source);
                    }
                    output.append("\n");
                }
            } else {
                output.append(result.getResponse().toString());
            }

            message.setText(output.toString());
            Transport.send(message);
        } catch (Exception e){
            throw new ElasticsearchException("Failed to send mail", e);
        }
        return true;
    }


}
