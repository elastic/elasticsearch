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

public class EmailAlertAction implements AlertAction {

    private final String displayField;
    private final List<Address> emailAddresses = new ArrayList<>();

    public EmailAlertAction(String displayField, String ... addresses){
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

}
