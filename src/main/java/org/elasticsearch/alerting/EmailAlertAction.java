/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.search.SearchHitField;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class EmailAlertAction implements AlertAction {
    List<String> emailAddresses = new ArrayList<>();

    String from = "esalertingtest@gmail.com";
    String passwd = "elasticsearchforthewin";
    String server = "smtp.gmail.com";
    int port = 465;



    public EmailAlertAction(SearchHitField hitField){
        emailAddresses.add("brian.murphy@elasticsearch.com");
    }

    public EmailAlertAction(String ... addresses){
        for (String address : addresses) {
            emailAddresses.add(address);
        }
    }


    @Override
    public boolean doAction(AlertResult result) {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");
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
                    InternetAddress.parse(emailAddresses.get(0)));
            message.setSubject("Elasticsearch Alert!");
            message.setText(result.searchResponse.toString());
            Transport.send(message);
        } catch (Exception e){
            throw new ElasticsearchException("Failed to send mail", e);
        }

        return true;
    }

    @Override
    public String getActionType() {
        return "email";
    }
}
