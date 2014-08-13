/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import java.util.List;

public class EmailAlertActionFactory implements AlertActionFactory{

    @Override
    public AlertAction createAction(Object parameters) {
        EmailAlertAction action = new EmailAlertAction();
        if (parameters instanceof List){
            for (String emailAddress : (List<String>)parameters) {
                action.addEmailAddress(emailAddress);
            }
        }
        return action;
    }
}
