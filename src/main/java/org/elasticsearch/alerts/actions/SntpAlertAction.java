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

public class SntpAlertAction implements AlertAction {

    private final String displayField;
    private final List<Address> emailAddresses = new ArrayList<>();

    public SntpAlertAction(String displayField, String... addresses){
        for (String address : addresses) {
            addEmailAddress(address);
        }
        this.displayField = displayField;
    }

    public void addEmailAddress(String address) {
        try {
            emailAddresses.add(InternetAddress.parse(address)[0]);
        } catch (AddressException addressException) {
            throw new ElasticsearchException("Unable to parse address : [" + address + "]");
        }
    }

   public String getDisplayField() {
        return displayField;
    }


    public List<Address> getEmailAddresses() {
        return new ArrayList<>(emailAddresses);
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SntpAlertAction that = (SntpAlertAction) o;

        if (displayField != null ? !displayField.equals(that.displayField) : that.displayField != null) return false;
        if (emailAddresses != null ? !emailAddresses.equals(that.emailAddresses) : that.emailAddresses != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = displayField != null ? displayField.hashCode() : 0;
        result = 31 * result + (emailAddresses != null ? emailAddresses.hashCode() : 0);
        return result;
    }

}
