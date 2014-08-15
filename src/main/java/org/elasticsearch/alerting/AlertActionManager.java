/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlertActionManager extends AbstractComponent {

    private final AlertManager alertManager;
    private final Map<String, AlertActionFactory> actionImplemented;

    @Inject
    public AlertActionManager(Settings settings, AlertManager alertManager) {
        super(settings);
        this.alertManager = alertManager;
        this.actionImplemented = new HashMap<>();
        registerAction("email", new EmailAlertActionFactory());
    }

    public void registerAction(String name, AlertActionFactory actionFactory){
        synchronized (actionImplemented) {
            actionImplemented.put(name, actionFactory);
        }
    }

    public List<AlertAction> parseActionsFromMap(Map<String,Object> actionMap) {
        List<AlertAction> actions = new ArrayList<>();
        synchronized (actionImplemented) {
            for (Map.Entry<String, Object> actionEntry : actionMap.entrySet()) {
                actions.add(actionImplemented.get(actionEntry.getKey()).createAction(actionEntry.getValue()));
            }
        }
        return actions;
    }

    public void doAction(String alertName, AlertResult alertResult){
        Alert alert = alertManager.getAlertForName(alertName);
        for (AlertAction action : alert.actions()) {
            action.doAction(alertName, alertResult);
        }
    }

}
