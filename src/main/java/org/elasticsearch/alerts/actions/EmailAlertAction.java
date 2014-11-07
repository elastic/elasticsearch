/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.triggers.TriggerResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmailAlertAction implements AlertAction {

    private String displayField = null;
    private List<Address> emailAddresses = new ArrayList<>();

    // TODO: Move to factory and make configurable
    int port = 587;
    String server = "smtp.gmail.com";
    String from = "esalertingtest@gmail.com";
    String passwd = "elasticsearchforthewin";

    public EmailAlertAction(String displayField, String ... addresses){
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
    public String getActionName() {
        return "email";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("addresses");
        builder.startArray();
        for (Address emailAddress : emailAddresses){
            builder.value(emailAddress.toString());
        }
        builder.endArray();
        if (displayField != null) {
            builder.field("display", displayField);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean doAction(Alert alert, TriggerResult result) {
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
            message.setSubject("Elasticsearch Alert " + alert.alertName() + " triggered");
            StringBuffer output = new StringBuffer();
            output.append("The following query triggered because " + result.getTrigger().toString() + "\n");
            output.append("The total number of hits returned : " + result.getResponse().getHits().getTotalHits() + "\n");
            output.append("For query : " + result.getRequest());
            output.append("\n");
            output.append("Indices : ");
            for (String index : result.getRequest().indices()) {
                output.append(index);
                output.append("/");
            }
            output.append("\n");
            output.append("\n");
            /*
            ///@TODO: FIX THE SEARCH RESULT DISPLAY STUFF
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
            */
            message.setText(output.toString());
            Transport.send(message);
        } catch (Exception e){
            throw new ElasticsearchException("Failed to send mail", e);
        }
        return true;
    }
}
