/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerting;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

public class TriggerManager extends AbstractComponent {

    private final AlertManager alertManager;

    public static AlertTrigger parseTriggerFromMap(Map<String, Object> triggerMap) {
        //For now just trigger on number of events greater than 1
        for (Map.Entry<String,Object> entry : triggerMap.entrySet()){
            AlertTrigger.TriggerType type = AlertTrigger.TriggerType.fromString(entry.getKey());

            AlertTrigger.SimpleTrigger simpleTrigger = AlertTrigger.SimpleTrigger.fromString(entry.getValue().toString().substring(0, 1));
            int value = Integer.valueOf(entry.getValue().toString().substring(1));
            return new AlertTrigger(simpleTrigger, type, value);
        }
        throw new ElasticsearchIllegalArgumentException();
    }

    @Inject
    public TriggerManager(Settings settings, AlertManager alertManager) {
        super(settings);
        this.alertManager = alertManager;
    }

    public boolean isTriggered(String alertName, SearchResponse response) {
        Alert alert = this.alertManager.getAlertForName(alertName);
        if (alert == null){
            logger.warn("Could not find alert named [{}] in alert manager perhaps it has been deleted.", alertName);
            return false;
        }
        long testValue;
        switch (alert.trigger().triggerType()) {
            case NUMBER_OF_EVENTS:
                testValue = response.getHits().getTotalHits();
                break;
            default:
                throw new ElasticsearchIllegalArgumentException("Bad value for trigger.triggerType [" + alert.trigger().triggerType() + "]");
        }
        int triggerValue = alert.trigger().value();
        //Move this to SimpleTrigger
        switch (alert.trigger().trigger()) {
            case GREATER_THAN:
                return testValue > triggerValue;
            case LESS_THAN:
                return testValue < triggerValue;
            case EQUAL:
                return testValue == triggerValue;
            case NOT_EQUAL:
                return testValue != triggerValue;
            case RISES_BY:
            case FALLS_BY:
                return false; //TODO FIX THESE
        }
        return false;
    }
}
