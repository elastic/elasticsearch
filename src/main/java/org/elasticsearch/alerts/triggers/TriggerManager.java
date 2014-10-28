/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.triggers;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Map;


/*
 * TODO : The trigger classes need cleanup and refactoring to be similar to the AlertActions and be pluggable
 */
public class TriggerManager extends AbstractComponent {

    private final AlertManager alertManager;
    private final ScriptService scriptService;

    public static AlertTrigger parseTrigger(XContentParser parser) throws IOException {
        AlertTrigger trigger = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                switch (currentFieldName) {
                    case "script":
                        String script = null;
                        ScriptService.ScriptType scriptType = null;
                        String scriptLang = null;
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token.isValue()) {
                                switch (currentFieldName) {
                                    case "script" :
                                        script = parser.text();
                                        break;
                                    case "script_type" :
                                        scriptType = ScriptService.ScriptType.valueOf(parser.text());
                                        break;
                                    case "script_lang" :
                                        scriptLang = parser.text();
                                        break;
                                    default:
                                        throw new ElasticsearchIllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                                }
                            }
                        }
                        trigger = new AlertTrigger(new ScriptedAlertTrigger(script, scriptType, scriptLang));
                        break;
                    default:
                        break;
                }
            } else if (token.isValue()) {
                String expression = parser.text();
                AlertTrigger.SimpleTrigger simpleTrigger = AlertTrigger.SimpleTrigger.fromString(expression.substring(0, 1));
                int value = Integer.valueOf(expression.substring(1));
                trigger = new AlertTrigger(simpleTrigger, AlertTrigger.TriggerType.NUMBER_OF_EVENTS, value);
            }
        }
        return trigger;
    }

    @Inject
    public TriggerManager(Settings settings, AlertManager alertManager, ScriptService scriptService) {
        super(settings);
        this.alertManager = alertManager;
        this.scriptService = scriptService;
    }

    public boolean doScriptTrigger(ScriptedAlertTrigger scriptTrigger, SearchResponse response) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder = response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            Map<String, Object> responseMap = XContentHelper.convertToMap(builder.bytes(), false).v2();

            ExecutableScript executable = scriptService.executable(scriptTrigger.scriptLang, scriptTrigger.script,
                    scriptTrigger.scriptType, responseMap);

            Object returnValue = executable.run();
            logger.warn("Returned [{}] from script", returnValue);
            if (returnValue instanceof Boolean) {
                return (Boolean) returnValue;
            } else {
                throw new ElasticsearchIllegalStateException("Trigger script [" + scriptTrigger.script + "] " +
                        "did not return a Boolean");
            }
        } catch (Exception e ){
            logger.error("Failed to execute script trigger", e);
        }
        return false;
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
            case SCRIPT:
                return doScriptTrigger(alert.trigger().scriptedTrigger(), response);
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
