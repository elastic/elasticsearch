/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.util.List;
import java.util.Map;

public class EmailAlertActionFactory implements AlertActionFactory{

    @Override
    public AlertAction createAction(Object parameters) {
        EmailAlertAction action = new EmailAlertAction();
        if (parameters instanceof List){
            for (String emailAddress : (List<String>)parameters) {
                action.addEmailAddress(emailAddress);
            }
        } else if (parameters instanceof Map) {
            Map<String,Object> paramMap = (Map<String,Object>)parameters;
            Object addresses = paramMap.get("addresses");
            if (addresses == null){
                throw new ElasticsearchException("Unable to parse email addresses from : " + parameters);
            }
            for (String emailAddress : (List<String>)addresses) {
                action.addEmailAddress(emailAddress);
            }
            Object displayField = paramMap.get("display");
            if (displayField != null){
                action.displayField(displayField.toString());
            }
        } else {
            throw new ElasticsearchIllegalArgumentException("Unable to parse [" + parameters + "] as an EmailAlertAction");
        }
        return action;
    }
}
