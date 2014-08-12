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
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHitField;

public class TriggerManager extends AbstractComponent {

    private final AlertManager alertManager;
    //private ESLogger logger = Loggers.getLogger(TriggerManager.class);

    public static AlertTrigger parseTriggerFromSearchField(SearchHitField hitField) {
        //For now just trigger on number of events greater than 1
        return new AlertTrigger(AlertTrigger.SimpleTrigger.GREATER_THAN, AlertTrigger.TriggerType.NUMBER_OF_EVENTS, 1);
        //return null;
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
        int testValue;
        switch (alert.trigger().triggerType()) {
            case NUMBER_OF_EVENTS:
                testValue = response.getHits().getHits().length;
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
