/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.search.SearchHitField;

import java.util.ArrayList;
import java.util.List;

public class EmailAlertAction implements AlertAction {
    List<String> emailAddresses = new ArrayList<>();

    String from = "esalertingtest@gmail.com";
    String passwd = "elasticsearchforthewin";
    String server = "smtp.gmail.com";
    int port = 465;



    public EmailAlertAction(SearchHitField hitField){
        emailAddresses.add("brian.murphy@elasticsearch.com");
    }

    @Override
    public boolean doAction(AlertResult alert) {
        //Email here
        return true;
    }

    @Override
    public String getActionType() {
        return "email";
    }
}
