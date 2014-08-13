/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class EmailAlertAction implements AlertAction {
    List<Address> emailAddresses = new ArrayList<>();
    String displayField = null;

    String from = "esalertingtest@gmail.com";
    String passwd = "elasticsearchforthewin";
    String server = "smtp.gmail.com";
    int port = 587;


    public EmailAlertAction(String ... addresses){
        for (String address : addresses) {
            addEmailAddress(address);
        }
    }

    public void addEmailAddress(String address) {
        try {
            emailAddresses.add(InternetAddress.parse(address)[0]);
        } catch (AddressException addressException) {
            throw new ElasticsearchException("Unable to parse address : [" + address + "]");
        }
    }

    public void displayField(String displayField){
        this.displayField = displayField;
    }

    @Override
    public boolean doAction(String alertName, AlertResult result) {
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
                    emailAddresses.toArray(new Address[1]));
            message.setSubject("Elasticsearch Alert " + alertName + " triggered");
            StringBuffer output = new StringBuffer();
            output.append("The following query triggered because " + result.trigger.toString() + "\n");
            output.append("The total number of hits returned : " + result.searchResponse.getHits().getTotalHits() + "\n");
            output.append("For query : " + XContentHelper.convertToJson(result.query.bytes(),true,true) + "\n");
            output.append("\n");
            output.append("Indices : ");
            for (String index : result.indices) {
                output.append(index);
                output.append("/");
            }
            output.append("\n");
            output.append("\n");
            if (displayField != null) {
                for (SearchHit sh : result.searchResponse.getHits().getHits()) {
                    if (sh.sourceAsMap().containsKey(displayField)) {
                        output.append(sh.sourceAsMap().get(displayField).toString());
                    } else {
                        output.append(new String(sh.source()));
                    }
                    output.append("\n");
                }
            } else {
                output.append(result.searchResponse.toString());
            }
            message.setText(output.toString());
            Transport.send(message);
        } catch (Exception e){
            throw new ElasticsearchException("Failed to send mail", e);
        }
        return true;
    }
}
