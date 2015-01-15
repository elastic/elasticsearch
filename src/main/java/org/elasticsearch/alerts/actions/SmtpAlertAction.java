/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.actions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.mail.*;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SmtpAlertAction implements AlertAction {

    private final List<Address> emailAddresses = new ArrayList<>();
    private final String subjectTemplate;
    private final String messageTemplate;

    public SmtpAlertAction(String subjectTemplate, String messageTemplate, String... addresses){
        for (String address : addresses) {
            addEmailAddress(address);
        }
        this.subjectTemplate = subjectTemplate;
        this.messageTemplate = messageTemplate;
    }

    public void addEmailAddress(String address) {
        try {
            emailAddresses.add(InternetAddress.parse(address)[0]);
        } catch (AddressException addressException) {
            throw new ElasticsearchException("Unable to parse address : [" + address + "]");
        }
    }

    public List<Address> getEmailAddresses() {
        return new ArrayList<>(emailAddresses);
    }

    public String getMessageTemplate() {
        return messageTemplate;
    }

    public String getSubjectTemplate() {
        return subjectTemplate;
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

        if (subjectTemplate != null) {
            builder.field("subject", subjectTemplate);
        }

        if (messageTemplate != null) {
            builder.field("message", messageTemplate);
        }

        builder.endObject();
        return builder;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SmtpAlertAction that = (SmtpAlertAction) o;

        if (emailAddresses != null ? !emailAddresses.equals(that.emailAddresses) : that.emailAddresses != null)
            return false;
        if (!messageTemplate.equals(that.messageTemplate)) return false;
        if (!subjectTemplate.equals(that.subjectTemplate)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = emailAddresses != null ? emailAddresses.hashCode() : 0;
        result = 31 * result + subjectTemplate.hashCode();
        result = 31 * result + messageTemplate.hashCode();
        return result;
    }



}
